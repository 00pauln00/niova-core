package main

import (
	"bytes"
	"common/lookout"
	"common/requestResponseLib"
	compressionLib "common/specificCompressionLib"
	"controlplane/serfagenthandler"
	"ctlplane/client_api"
	"encoding/gob"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/google/uuid"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"unsafe"
	"time"
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

type nisdMonitor struct {
	udpPort       string
	storageClient client_api.ClientAPI
	udpSocket     net.PacketConn
	lookout       lookout.EPContainer
	endpointRoot  *string
	httpPort      *int

	//serf
	serfHandler    serfagenthandler.SerfAgentHandler
	agentName       string
	addr            string
	agentPort       string
	agentRPCPort    string
	gossipNodesPath string
	serfLogger      string
}

//NISD
type udpMessage struct {
	addr    net.Addr
	message []byte
}

func usage(rc int) {
	fmt.Printf("Usage: [OPTIONS] %s\n", os.Args[0])
	flag.PrintDefaults()
	os.Exit(rc)
}

func (handler *nisdMonitor) init() {
	var (
		showHelp      *bool
		showHelpShort *bool
	)

	handler.endpointRoot = flag.String("dir", "/tmp/.niova", "endpoint directory root")
	handler.httpPort = flag.Int("port", 8081, "http listen port")
	showHelpShort = flag.Bool("h", false, "")
	showHelp = flag.Bool("help", false, "print help")
	flag.StringVar(&handler.udpPort, "u", "1054", "UDP port for NISD communication")
	flag.StringVar(&handler.agentName, "n", uuid.New().String(), "Agent name")
	flag.StringVar(&handler.addr, "a", "127.0.0.1", "Agent addr")
	flag.StringVar(&handler.agentPort, "p", "3991", "Agent port for serf")
	flag.StringVar(&handler.agentRPCPort, "r", "3992", "Agent RPC port")
	flag.StringVar(&handler.gossipNodesPath, "c", "./gossipNodes", "PMDB server gossip info")
	flag.StringVar(&handler.serfLogger, "s", "serf.log", "Serf logs")
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

//NISD
func (handler *nisdMonitor) getConfigNSend(udpInfo udpMessage) {
	//Get uuid from the byte array
	data := udpInfo.message
	uuidString := string(data[:36])
	uuidHex, _ := uuid.Parse(uuidString)

	//FIXME
	nisd, ok := handler.lookout.EpMap[uuidHex]
	if ok {
		if !nisd.Alive {
			/*
			nisd.pendingCmds = make(map[string]*epCommand)
			nisd.Alive = true
			nisd.LastReport = time.Now()
			*/
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
	responseByte := handler.storageClient.Request(requestByte.Bytes(), "", false)

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
	handler.udpSocket.WriteTo(returnData, udpInfo.addr)
}

//NISD
func (handler *nisdMonitor) startSerfAgent() error {
	switch handler.serfLogger {
	case "ignore":
		log.SetOutput(ioutil.Discard)
	default:
		f, err := os.OpenFile(handler.serfLogger, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
		if err != nil {
			log.SetOutput(os.Stderr)
		} else {
			log.SetOutput(f)
		}
	}

	handler.serfHandler = serfagenthandler.SerfAgentHandler{}
	handler.serfHandler.Name = handler.agentName
	handler.serfHandler.BindAddr = handler.addr
	handler.serfHandler.BindPort, _ = strconv.Atoi(handler.agentPort)
	handler.serfHandler.AgentLogger = log.Default()
	handler.serfHandler.RpcAddr = handler.addr
	handler.serfHandler.RpcPort = handler.agentRPCPort
	joinAddrs, err := serfagenthandler.GetPeerAddress(handler.gossipNodesPath)
	if err != nil {
		return err
	}
	//Start serf agent
	_, err = handler.serfHandler.Startup(joinAddrs, true)
	return err
}

//NISD
func (handler *nisdMonitor) getCompressedGossipDataNISD() map[string]string {
	returnMap := make(map[string]string)
	for _, nisd := range handler.lookout.EpMap {
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
	return returnMap
}

//NISD
func (handler *nisdMonitor) setTags() {
	for {
		tagData := handler.getCompressedGossipDataNISD()
		err := handler.serfHandler.SetTags(tagData)
		if err != nil {
			fmt.Println(err)
		}
		time.Sleep(10 * time.Second)
	}
}

//NISD
func (handler *nisdMonitor) startClientAPI() {
	//Init niovakv client API
	handler.storageClient = client_api.ClientAPI{
		Timeout: 10,
	}
	stop := make(chan int)
	go func() {
		err := handler.storageClient.Start(stop, handler.gossipNodesPath)
		if err != nil {
			fmt.Println("Error while starting client API : ", err)
			os.Exit(1)
		}
	}()
	handler.storageClient.Till_ready()
}

//NISD
func (handler *nisdMonitor) startUDPListner() {
	var err error
	handler.udpSocket, err = net.ListenPacket("udp", ":"+handler.udpPort)
	if err != nil {
		fmt.Println("UDP listner failed : ", err)
	}

	defer handler.udpSocket.Close()
	for {
		buf := make([]byte, 1024)
		_, addr, err := handler.udpSocket.ReadFrom(buf)
		if err != nil {
			continue
		}
		udpInfo := udpMessage{
			addr:    addr,
			message: buf,
		}
		go handler.getConfigNSend(udpInfo)
	}
}


func main() {
	var nisd nisdMonitor

	//Get cmd line args
	nisd.init()

	//Start pmdb service client discovery api
	nisd.startClientAPI()

	//Start serf agent
	nisd.startSerfAgent()

	//Start udp listener
	go nisd.startUDPListner()

	//Set serf tags
	go nisd.setTags()

	//Start lookout process
	nisd.lookout = lookout.EPContainer{}
	nisd.lookout.Start()
}
