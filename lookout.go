package main

//import _ "net/http/pprof"

import (
	"bytes"
	"common/requestResponseLib"
	"controlplane/serfagenthandler"
	"ctlplane/client_api"
	"encoding/gob"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/fsnotify/fsnotify"
	"github.com/google/uuid"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	compressionLib "common/specificCompressionLib"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

// #include <unistd.h>
// #include <string.h>
// //#include <errno.h>
// //int usleep(useconds_t usec);
/*
#define INET_ADDRSTRLEN 16
#define UUID_LEN 37
struct nisd_config
{
  char  nisd_uuid[UUID_LEN];
  char  nisd_ipaddr[INET_ADDRSTRLEN];
  int   nisdc_addr_len;
  int   nisd_port;
};
*/
import "C"

func mlongsleep() {
	C.usleep(500000)
}

//FIXME : Use struct
var (
	endpointRoot    *string
	httpPort        *int
	showHelp        *bool
	showHelpShort   *bool
	udpPort         string
	agentHandler    serfagenthandler.SerfAgentHandler
	agentName       string
	addr            string
	agentPort       string
	agentRPCPort    string
	gossipNodesPath string
	serfLogger      string
	appType		string
)

func usage(rc int) {
	fmt.Printf("Usage: [OPTIONS] %s\n", os.Args[0])
	flag.PrintDefaults()
	os.Exit(rc)
}

func init() {
	endpointRoot = flag.String("dir", "/tmp/.niova", "endpoint directory root")
	httpPort = flag.Int("port", 8081, "http listen port")
	showHelpShort = flag.Bool("h", false, "")
	showHelp = flag.Bool("help", false, "print help")
	flag.StringVar(&udpPort, "u", "1054", "UDP port for NISD communication")
	flag.StringVar(&agentName, "n", uuid.New().String(), "Agent name")
	flag.StringVar(&addr, "a", "127.0.0.1", "Agent addr")
	flag.StringVar(&agentPort, "p", "3991", "Agent port for serf")
	flag.StringVar(&agentRPCPort, "r", "3992", "Agent RPC port")
	flag.StringVar(&gossipNodesPath, "c", "./gossipNodes", "PMDB server gossip info")
	flag.StringVar(&serfLogger, "s", "serf.log", "Serf logs")
	flag.StringVar(&appType, "ap", "NISD", "App type [PMDB,NISD]")
	flag.Parse()

	nonParsed := flag.Args()
	if len(nonParsed) > 0 {
		fmt.Println("Unexpected argument found:", nonParsed[1])
		usage(1)
	}

	if *showHelpShort == true || *showHelp == true {
		usage(0)
	}
}

type epContainer struct {
	EpMap         map[uuid.UUID]*NcsiEP
	Mutex         sync.Mutex
	Path          string
	run           bool
	Statb         syscall.Stat_t
	EpWatcher     *fsnotify.Watcher
	serfHandler   serfagenthandler.SerfAgentHandler
	udpEvent      chan udpMessage
	storageClient client_api.ClientAPI
	udpSocket     net.PacketConn
	httpQuery     map[string](chan []byte)
}

type udpMessage struct {
	addr    net.Addr
	message []byte
}

type NisdConfig struct {
	Uuid   string
	Ipaddr string
	Port   int
}

func (epc *epContainer) tryAdd(uuid uuid.UUID) {
	lns := epc.EpMap[uuid]
	if lns == nil {
		newlns := NcsiEP{
			Uuid:         uuid,
			Path:         epc.Path + "/" + uuid.String(),
			Name:         "r-a4e1",
			NiovaSvcType: "raft",
			Port:         6666,
			LastReport:   time.Now(),
			LastClear:    time.Now(),
			Alive:        true,
			pendingCmds:  make(map[string]*epCommand),
		}

		if err := epc.EpWatcher.Add(newlns.Path + "/output"); err != nil {
			log.Fatalln("Watcher.Add() failed:", err)
		}

		// serialize with readers in httpd context, this is the only
		// writer thread so the lookup above does not require a lock
		epc.Mutex.Lock()
		epc.EpMap[uuid] = &newlns
		epc.Mutex.Unlock()
		log.Printf("added: %+v\n", newlns)
	}
}

func (epc *epContainer) Scan() {
	files, err := ioutil.ReadDir(epc.Path)
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		// Need to support removal of stale items
		if uuid, err := uuid.Parse(file.Name()); err == nil {
			epc.tryAdd(uuid)
		}
	}
}

func (epc *epContainer) Monitor() error {
	var err error = nil
	err = epc.serfAgentStart()
	if err != nil {
		log.Printf("Serf error : %s", err)
		return err
	}

	for epc.run == true {
		var tmp_stb syscall.Stat_t

		err = syscall.Stat(epc.Path, &tmp_stb)
		if err != nil {
			log.Printf("syscall.Stat('%s'): %s", epc.Path, err)
			break
		}

		if tmp_stb.Mtim != epc.Statb.Mtim {
			epc.Statb = tmp_stb
			epc.Scan()
		}

		// Query for liveness
		for _, ep := range epc.EpMap {
			ep.Remove()
			ep.Detect(appType)
		}

		//Update tags
		epc.setTags()

		mlongsleep()
	}

	return err
}

func (epc *epContainer) JsonMarshalUUID(uuid uuid.UUID) []byte {
	var jsonData []byte
	var err error

	epc.Mutex.Lock()
	ep := epc.EpMap[uuid]
	epc.Mutex.Unlock()
	if ep != nil {
		jsonData, err = json.MarshalIndent(ep, "", "\t")
	} else {
		// Return an empty set if the item does not exist
		jsonData = []byte("{}")
	}

	if err != nil {
		return nil
	}

	return jsonData
}

func (epc *epContainer) JsonMarshal() []byte {
	var jsonData []byte

	epc.Mutex.Lock()
	jsonData, err := json.MarshalIndent(epc.EpMap, "", "\t")
	epc.Mutex.Unlock()

	if err != nil {
		return nil
	}

	return jsonData
}

func (epc *epContainer) processInotifyEvent(event *fsnotify.Event) {
	splitPath := strings.Split(event.Name, "/")
	cmpstr := splitPath[len(splitPath)-1]

	uuid, err := uuid.Parse(splitPath[len(splitPath)-3])
        if err != nil {
                return
        }

	//Check if its for HTTP
	httpID := strings.Split(cmpstr, "HTTP")
	if len(httpID) == 2 {
		var output []byte
		if ep := epc.EpMap[uuid]; ep != nil {
		        ep.Complete(cmpstr, &output)
		}
		epc.httpQuery[httpID[1]] <- output
		return
	}

	if ep := epc.EpMap[uuid]; ep != nil {
		ep.Complete(cmpstr, nil)
	}
}

func (epc *epContainer) getConfigNSend(udpInfo udpMessage) {
	//Get uuid from the byte array
	data := udpInfo.message
	uuidString := string(data[:36])
	uuidHex, _ := uuid.Parse(uuidString)
	nisd, ok := epc.EpMap[uuidHex]
	if ok {
		if !nisd.Alive {
			nisd.pendingCmds = make(map[string]*epCommand)
			nisd.Alive = true
			nisd.LastReport = time.Now()
		}
	}

	//Send config read request to PMDB server
	request := requestResponseLib.KVRequest{
		Operation: "read",
		Key:       string(uuidString),
	}
	var requestByte bytes.Buffer
	enc := gob.NewEncoder(&requestByte)
	enc.Encode(request)
	responseByte := epc.storageClient.Request(requestByte.Bytes(), "", false)

	//Decode response to IPAddr and Port
	responseObj := requestResponseLib.KVResponse{}
	dec := gob.NewDecoder(bytes.NewBuffer(responseByte))
	dec.Decode(&responseObj)
	var value map[string]string
	json.Unmarshal(responseObj.Value, &value)
	ipaddr := value["IP_ADDR"]
	port, _ := strconv.Atoi(value["Port"])

	//Fill C structure,Add statement for deleting the allocatted buffer
	nisd_peer_config := C.struct_nisd_config{}
	C.strncpy(&(nisd_peer_config.nisd_uuid[0]), C.CString(uuidString), C.ulong(len(uuidString)+1))
	C.strncpy(&(nisd_peer_config.nisd_ipaddr[0]), C.CString(ipaddr), C.ulong(len(ipaddr)+1))
	nisd_peer_config.nisdc_addr_len = C.int(len(ipaddr))
	nisd_peer_config.nisd_port = C.int(port)
	returnData := C.GoBytes(unsafe.Pointer(&nisd_peer_config), C.sizeof_struct_nisd_config)

	//Send the data to the node
	epc.udpSocket.WriteTo(returnData, udpInfo.addr)
}

func (epc *epContainer) epOutputWatcher() {
	for {
		select {
		case event := <-epc.EpWatcher.Events:

			if event.Op == fsnotify.Create {
				epc.processInotifyEvent(&event)
			}

			// watch for errors
		case err := <-epc.EpWatcher.Errors:
			fmt.Println("ERROR", err)

		case udpInfo := <-epc.udpEvent:
			go epc.getConfigNSend(udpInfo)

		}
	}
}

func (epc *epContainer) Init(path string) error {
	// Check the provided endpoint root path
	err := syscall.Stat(path, &epc.Statb)
	if err != nil {
		return err
	}

	// Set path (Xxx still need to check if this is a directory or not)
	epc.Path = path

	// Create the map
	epc.EpMap = make(map[uuid.UUID]*NcsiEP)
	if epc.EpMap == nil {
		return syscall.ENOMEM
	}

	epc.EpWatcher, err = fsnotify.NewWatcher()
	if err != nil {
		return err
	}

	epc.run = true

	go epc.epOutputWatcher()

	return nil
}

func (epc *epContainer) httpHandleRootRequest(w http.ResponseWriter) {
	fmt.Fprintf(w, "%s\n", string(epc.JsonMarshal()))
}

func (epc *epContainer) httpHandleUUIDRequest(w http.ResponseWriter,
	uuid uuid.UUID) {

	fmt.Fprintf(w, "%s\n", string(epc.JsonMarshalUUID(uuid)))
}

func (epc *epContainer) httpHandleRoute(w http.ResponseWriter, r *url.URL) {
	// Splitting '/vX/' results in a length of 2
	splitURL := strings.Split(r.String(), "/v0/")


	if len(splitURL) == 2 && len(splitURL[1]) == 0 {
		epc.httpHandleRootRequest(w)

	} else if uuid, err := uuid.Parse(splitURL[1]); err == nil {
		epc.httpHandleUUIDRequest(w, uuid)

	} else {
		fmt.Fprintln(w, "Invalid request: url", splitURL[1])
	}

}

func (epc *epContainer) HttpHandle(w http.ResponseWriter, r *http.Request) {
	epc.httpHandleRoute(w, r.URL)
}


func (epc *epContainer) customQueryNISD(nisdUUID uuid.UUID, query string) []byte {
	epc.Mutex.Lock()
        ep, ok := epc.EpMap[nisdUUID]
        epc.Mutex.Unlock()
	//If not present
	if !ok {
		return []byte("Specified NISD is not present")
	}

	httpID := "HTTP_"+uuid.New().String()
	epc.httpQuery[httpID] = make(chan []byte, 1)
	ep.CustomQuery(query, httpID)

	//FIXME: Have select in case of NISD dead
	var byteOP []byte
	select {
	case byteOP = <- epc.httpQuery[httpID]:
		break

	}
	return byteOP
}

func (epc *epContainer) QueryNISDHandle(w http.ResponseWriter, r *http.Request) {

	//Decode the NISD request structure
	requestBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println(err)
	}

	requestObj := requestResponseLib.LookoutRequest{}
        dec := gob.NewDecoder(bytes.NewBuffer(requestBytes))
        err = dec.Decode(&requestObj)
        if err != nil {
                log.Println(err)
        }

	//Call the appropriate function
	nisdUUID := requestObj.NISD
	query := requestObj.Cmd
	output := epc.customQueryNISD(nisdUUID, query)
	//Data to writer
	w.Write(output)
}


func (epc *epContainer) serveHttp() {
	http.HandleFunc("/v1/", epc.QueryNISDHandle)
	http.HandleFunc("/v0/", epc.HttpHandle)
	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(*httpPort), nil))
}

func (epc *epContainer) serfAgentStart() error {
	switch serfLogger {
	case "ignore":
		log.SetOutput(ioutil.Discard)
	default:
		f, err := os.OpenFile(serfLogger, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
		if err != nil {
			log.SetOutput(os.Stderr)
		} else {
			log.SetOutput(f)
		}
	}

	epc.serfHandler = serfagenthandler.SerfAgentHandler{}
	epc.serfHandler.Name = agentName
	epc.serfHandler.BindAddr = addr
	epc.serfHandler.BindPort, _ = strconv.Atoi(agentPort)
	epc.serfHandler.AgentLogger = log.Default()
	epc.serfHandler.RpcAddr = addr
	epc.serfHandler.RpcPort = agentRPCPort
	joinAddrs, err := serfagenthandler.GetPeerAddress(gossipNodesPath)
	if err != nil {
		return err
	}
	//Start serf agent
	_, err = epc.serfHandler.Startup(joinAddrs, true)
	return err
}

func (epc *epContainer) getCompressedGossipDataNISD() map[string]string {
	returnMap := make(map[string]string)
	for _,nisd := range epc.EpMap{
		//Get data from map
		uuid := nisd.Uuid.String()
		status := nisd.Alive
		//Compact the data
		cuuid, _ := compressionLib.CompressUUID(uuid)
		cstatus := "0"
		if status {
			cstatus = "1"
		}

		//Fill map; will add extra info in future
		returnMap[cuuid] = cstatus
	}
	returnMap["Type"] = "LOOKOUT"
	returnMap["Hport"] = strconv.Itoa(*httpPort)
	return returnMap
}

func (epc *epContainer) setTags() {
	tagData := epc.getCompressedGossipDataNISD()
	err := epc.serfHandler.SetTags(tagData)
	if err != nil {
		fmt.Println(err)
	}
}

func (epc *epContainer) startClientAPI() {
	//Init niovakv client API
	epc.storageClient = client_api.ClientAPI{
		Timeout: 10,
	}
	stop := make(chan int)
	go func() {
		err := epc.storageClient.Start(stop, gossipNodesPath)
		if err != nil {
			fmt.Println("Error while starting client API : ", err)
			os.Exit(1)
		}
	}()
	epc.storageClient.Till_ready()
}

func (epc *epContainer) startUDPListner() {
	fmt.Println("Starting udp listner")
	//epc.udpEvent = make(chan udpMessage, 10)
	var err error
	epc.udpSocket, err = net.ListenPacket("udp", ":"+udpPort)
	if err != nil {
		fmt.Println("UDP listner failed : ", err)
	}

	defer epc.udpSocket.Close()
	for {
		buf := make([]byte, 1024)
		_, addr, err := epc.udpSocket.ReadFrom(buf)
		if err != nil {
			continue
		}
		udpInfo := udpMessage{
			addr:    addr,
			message: buf,
		}
		go epc.getConfigNSend(udpInfo)
	}
}

func main() {
	var epc epContainer

	if err := epc.Init(*endpointRoot); err != nil {
		log.Fatalf("epc.Init('%s'): %s", *endpointRoot, err)
	}

	epc.Scan()
	epc.udpEvent = make(chan udpMessage, 10)
	go epc.serveHttp()

	epc.startClientAPI()

	go epc.startUDPListner()

	epc.Monitor()
}
