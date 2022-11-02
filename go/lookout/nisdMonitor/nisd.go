package main

import (
	"bytes"
	"common/httpClient"
	"common/lookout"
	"common/requestResponseLib"
	"common/serviceDiscovery"
	compressionLib "common/specificCompressionLib"
	"controlplane/serfAgent"
	"encoding/gob"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/google/uuid"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
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

type nisdMonitor struct {
	udpPort       string
	storageClient serviceDiscovery.ServiceDiscoveryHandler
	udpSocket     net.PacketConn
	lookout       lookout.EPContainer
	endpointRoot  *string
	httpPort      int
	ctlPath       *string
	//serf
	serfHandler     serfAgent.SerfAgentHandler
	agentName       string
	addr            string
	agentPort       int16
	agentRPCPort    int16
	gossipNodesPath string
	serfLogger      string
	raftUUID	string
	PortRange	[]int16
	ServicePortRangeS int16
	ServicePortRangeE int16
}

var RecvdPort int

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

func (handler *nisdMonitor) parseCMDArgs() {
	var (
		showHelp      *bool
		showHelpShort *bool
	)

	handler.ctlPath = flag.String("dir", "/tmp/.niova", "endpoint directory root")
	showHelpShort = flag.Bool("h", false, "")
	showHelp = flag.Bool("help", false, "print help")

	flag.StringVar(&handler.udpPort, "u", "1054", "UDP port for NISD communication")
	flag.StringVar(&handler.agentName, "n", uuid.New().String(), "Agent name")
	flag.StringVar(&handler.addr, "a", "127.0.0.1", "Agent addr")
	flag.StringVar(&handler.gossipNodesPath, "c", "./gossipNodes", "PMDB server gossip info")
	flag.StringVar(&handler.serfLogger, "s", "serf.log", "Serf logs")
	flag.StringVar(&handler.raftUUID, "r", "", "Raft UUID")
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

func (handler *nisdMonitor) requestPMDB(key string) ([]byte, error) {
	request := requestResponseLib.KVRequest{
		Operation: "read",
		Key:       key,
	}
	var requestByte bytes.Buffer
	enc := gob.NewEncoder(&requestByte)
	enc.Encode(request)
	responseByte, err := handler.storageClient.Request(requestByte.Bytes(), "", false)
	return responseByte, err
}

func fillNisdCStruct(UUID string, ipaddr string, port int) []byte {
	//FIXME: free the memory
	nisd_peer_config := C.struct_nisd_config{}
	C.strncpy(&(nisd_peer_config.nisd_uuid[0]), C.CString(UUID), C.ulong(len(UUID)+1))
	C.strncpy(&(nisd_peer_config.nisd_ipaddr[0]), C.CString(ipaddr), C.ulong(len(ipaddr)+1))
	nisd_peer_config.nisdc_addr_len = C.int(len(ipaddr))
	nisd_peer_config.nisd_port = C.int(port)
	returnData := C.GoBytes(unsafe.Pointer(&nisd_peer_config), C.sizeof_struct_nisd_config)
	return returnData
}

//NISD
func (handler *nisdMonitor) getConfigNSend(udpInfo udpMessage) {
	//Get uuid from the byte array
	data := udpInfo.message
	uuidString := string(data[:36])

	//FIXME: Mark nisd as alive if reported dead
	handler.lookout.MarkAlive(uuidString)

	//Send config read request to PMDB server
	responseByte, _ := handler.requestPMDB(uuidString)

	//Decode response to IPAddr and Port
	responseObj := requestResponseLib.KVResponse{}
	dec := gob.NewDecoder(bytes.NewBuffer(responseByte))
	dec.Decode(&responseObj)
	var value map[string]string
	json.Unmarshal(responseObj.Value, &value)
	ipaddr := value["IP_ADDR"]
	port, _ := strconv.Atoi(value["Port"])

	//Fill C structure
	structByteArray := fillNisdCStruct(uuidString, ipaddr, port)

	//Send the data to the node
	handler.udpSocket.WriteTo(structByteArray, udpInfo.addr)
}

func setLogOutput(logPath string) {
	switch logPath {
	case "ignore":
		log.SetOutput(ioutil.Discard)
	default:
		f, err := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
		if err != nil {
			log.SetOutput(os.Stderr)
		} else {
			log.SetOutput(f)
		}
	}
}

func (handler *nisdMonitor) startSerfAgent() error {
	setLogOutput(handler.serfLogger)
	//agentPort := handler.agentPort
	handler.serfHandler = serfAgent.SerfAgentHandler{
		Name:        handler.agentName,
		BindAddr:    net.ParseIP(handler.addr),
		ServicePortRangeS: uint16(handler.ServicePortRangeS),
		ServicePortRangeE: uint16(handler.ServicePortRangeE),
		//BindPort:    uint16(agentPort),
		AgentLogger: log.Default(),
		RpcAddr:     net.ParseIP(handler.addr),
		//RpcPort:     uint16(handler.agentRPCPort),
	}

	joinAddrs, err := serfAgent.GetPeerAddress(handler.gossipNodesPath)
	if err != nil {
		return err
	}

	//Start serf agent
	_, err = handler.serfHandler.SerfAgentStartup(joinAddrs, true)
	return err
}

func (handler *nisdMonitor) getCompressedGossipDataNISD() map[string]string {
	returnMap := make(map[string]string)
	nisdMap := handler.lookout.GetList()
	for _, nisd := range nisdMap {
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
	httpPort  := RecvdPort
	returnMap["Type"] = "LOOKOUT"
	returnMap["Hport"] = strconv.Itoa(httpPort)
	return returnMap  
}

//NISD
func (handler *nisdMonitor) setTags() {
	for {
		tagData := handler.getCompressedGossipDataNISD()
		err := handler.serfHandler.SetNodeTags(tagData)
		if err != nil {
			fmt.Println(err)
		}
		time.Sleep(10 * time.Second)
	}
}

//NISD
func (handler *nisdMonitor) startClientAPI() {
	//Init niovakv client API
	handler.storageClient = serviceDiscovery.ServiceDiscoveryHandler{
		HTTPRetry: 10,
		SerfRetry: 5,
	}
	stop := make(chan int)
	go func() {
		err := handler.storageClient.StartClientAPI(stop, handler.gossipNodesPath)
		if err != nil {
			fmt.Println("Error while starting client API : ", err)
			os.Exit(1)
		}
	}()
	handler.storageClient.TillReady()
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

func (handler *nisdMonitor) SerfMembership() map[string]bool {
	membership := handler.storageClient.GetMembership()
	returnMap := make(map[string]bool)
	for _, member := range membership {
		if member.Status == "alive" {
			returnMap[member.Name] = true
		}
	}
	return returnMap
}

func (handler *nisdMonitor) getPortRange() error {
	var response requestResponseLib.PMDBKVResponse

	responseBytes, err := handler.requestPMDB(handler.raftUUID + "_Port_Range")
	if err != nil {
		fmt.Println("Request PMDB - ", err)
		return err
	}

	dec := gob.NewDecoder(bytes.NewBuffer(responseBytes))
	dec.Decode(&response)

	err = handler.getConfigData(string(response.ResultMap[handler.raftUUID+"_Port_Range"]))
	if err != nil {
		fmt.Println("getConfigData - ", err)
	}

	return nil
}

func (handler *nisdMonitor) getConfigData(config string) error {
	portRangeStart, err := strconv.Atoi(strings.Split(config,"-")[0])
	portRangeEnd, err := strconv.Atoi(strings.Split(config,"-")[1])
	if err != nil {
		return err
	}

	for i:=portRangeStart; i<=portRangeEnd; i++ {
		handler.PortRange = append(handler.PortRange, int16(i))
	}

	if len(handler.PortRange) < 3 {
		return errors.New("Not enough ports available in the specified range to start services")
	}

	return err
}

func (handler *nisdMonitor) checkHTTPLiveness() {
	var emptyByteArray []byte
	for {
		_, err := httpClient.HTTP_Request(emptyByteArray, "127.0.0.1:"+strconv.Itoa(int(RecvdPort)) + "/check", false)
		if err != nil {
			fmt.Println("HTTP Liveness - ", err)
		} else {
			fmt.Println("HTTP Liveness - HTTP Server is alive")
			break
		}
		time.Sleep(1 * time.Second)
	}
}

func (handler *nisdMonitor) findFreePort() int {
	for i := 0; i<len(handler.PortRange); i++ {
		handler.httpPort = int(handler.PortRange[i])
		fmt.Println("Trying to bind with - ", int(handler.httpPort))
		check, err := net.Listen("tcp", handler.addr + ":" + strconv.Itoa(int(handler.httpPort)))
		if err != nil {
			if strings.Contains(err.Error(), "bind") {
				continue
			} else {
				fmt.Println("Error while finding port : ", err)
				return 0
			}
		} else {
			check.Close()
			break
		}
	}
	fmt.Println("Returning free port - ", handler.httpPort)
	return handler.httpPort
}

func main() {
	var nisd nisdMonitor

	//Get cmd line args
	nisd.parseCMDArgs()

	//Start pmdb service client discovery api
	nisd.startClientAPI()

	err := nisd.getPortRange()
	if err != nil {
		os.Exit(1)
	}

	//Start serf agent
	nisd.ServicePortRangeS = nisd.PortRange[0]
	nisd.ServicePortRangeE = nisd.PortRange[len(nisd.PortRange)-1]
	err = nisd.startSerfAgent()
	if err != nil {
		fmt.Println("Error while starting serf agent : ", err)
		os.Exit(1)
	}

	//Start udp listener
	go nisd.startUDPListner()

	portAddr := &RecvdPort

	//Start lookout monitoring
	nisd.lookout = lookout.EPContainer{
		MonitorUUID:      "*",
		AppType:          "NISD",
		//HttpPort:         nisd.findFreePort(),
		PortRange:	  nisd.PortRange,
		CTLPath:          *nisd.ctlPath,
		SerfMembershipCB: nisd.SerfMembership,
		EnableHttp:       true,
		RetPort:	  portAddr,
	}
	errs := make(chan error, 1)
	go func() {
		errs <- nisd.lookout.Start()
	}()
	select {
	case err = <-errs:
		fmt.Println("Error while starting Lookout : ", err)
		os.Exit(1)
	default:
		fmt.Println("Lookout started successfully")
	}

	//Wait till http lookout http is up and running
	nisd.checkHTTPLiveness()

	//Set serf tags
	nisd.setTags()
}
