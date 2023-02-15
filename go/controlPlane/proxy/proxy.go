package main

import (
	//"bufio"
	"bufio"
	"bytes"
	"common/httpClient"
	"common/httpServer"
	"common/requestResponseLib"
	"common/serfAgent"
	compressionLib "common/specificCompressionLib"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	defaultLogger "log"
	"net"
	pmdbClient "niova/go-pumicedb-lib/client"
	PumiceDBCommon "niova/go-pumicedb-lib/common"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	uuid "github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
)

//Structure for proxy
type proxyHandler struct {
	//Other
	configPath string
	logLevel   string

	//Niovakvserver
	addr     net.IP
	addrList []net.IP

	//Pmdb nivoa client
	raftUUID                uuid.UUID
	clientUUID              uuid.UUID
	logPath                 string
	PMDBServerConfigArray   []PumiceDBCommon.PeerConfigData
	PMDBServerConfigByteMap map[string][]byte
	pmdbClientObj           *pmdbClient.PmdbClientObj

	//Serf agent
	serfAgentName     string
	serfAgentPort     uint16
	serfAgentRPCPort  uint16
	serfPeersFilePath string
	serfLogger        string
	serfAgentObj      serfAgent.SerfAgentHandler

	//Http
	httpPort      uint16
	limit         string
	requireStat   string
	httpServerObj httpServer.HTTPServerHandler

	//Port range
	ServicePortRangeS uint16
	ServicePortRangeE uint16
	portRange         []uint16
}

var MaxPort = 60000
var MinPort = 1000
var RecvdPort int

func usage() {
	flag.PrintDefaults()
	os.Exit(0)
}

/*
Structure : proxyHandler
Method    : getCmdParams
Arguments : None
Return(s) : None

Description : Function to get command line parameters while starting of the proxy.
*/
func (handler *proxyHandler) getCmdParams() {
	var tempRaftUUID, tempClientUUID string

	//Prepare default logpath
	defaultLogPath := "/" + "tmp" + "/" + "niovaKVServer" + ".log"
	flag.StringVar(&tempRaftUUID, "r", "NULL", "Raft UUID")
	flag.StringVar(&tempClientUUID, "u", uuid.NewV4().String(), "Client UUID")
	flag.StringVar(&handler.logPath, "l", defaultLogPath, "Log filepath")
	flag.StringVar(&handler.configPath, "c", "./", "Configuration file path")
	flag.StringVar(&handler.serfAgentName, "n", "NULL", "Serf agent name")
	flag.StringVar(&handler.limit, "e", "500", "Number of concurrent HTTP connections")
	flag.StringVar(&handler.serfLogger, "sl", "ignore", "Serf logger file [default:ignore]")
	flag.StringVar(&handler.logLevel, "ll", "", "Set log level for the execution")
	flag.StringVar(&handler.requireStat, "s", "0", "HTTP server stat : provides status of requests, If needed provide 1")
	flag.StringVar(&handler.serfPeersFilePath, "pa", "NULL", "Path to pmdb server serf configuration file")
	flag.Parse()
	handler.raftUUID, _ = uuid.FromString(tempRaftUUID)
	//FIXME: For testing purpose
	handler.clientUUID, _ = uuid.FromString(tempClientUUID)
}

func makeRange(min, max uint16) []uint16 {
	a := make([]uint16, max-min+1)
	for i := range a {
		a[i] = uint16(min + uint16(i))
	}
	return a
}

func removeDuplicateStr(strSlice []string) []string {
	allKeys := make(map[string]bool)
	list := []string{}
	for _, item := range strSlice {
		if _, value := allKeys[item]; !value {
			allKeys[item] = true
			list = append(list, item)
		}
	}
	return list
}

/*
Structure : proxyHandler
Method    : getConfigData
Arguments : []byte
Return(s) : error

Description : Parses proxy's configuration from PMDB server

Config should contain following:
Addr //Addr for serf agent and http listening
Aport //Serf agent-agent communication
Rport //Serf agent-client communication
Hport //Http listener port

*/
func (handler *proxyHandler) getConfigData() error {
	//Get addrs and Rports and store it in handler

	if _, err := os.Stat(handler.serfPeersFilePath); os.IsNotExist(err) {
		return err
	}
	reader, err := os.OpenFile(handler.serfPeersFilePath, os.O_RDONLY, 0444)
	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(reader)
	//Read IPAddrs
	scanner.Scan()
	IPAddrsTxt := strings.Split(scanner.Text(), " ")
	IPAddrs := removeDuplicateStr(IPAddrsTxt)
	for i := range IPAddrs {
		ipAddr := net.ParseIP(IPAddrs[i])
		if ipAddr == nil {
			continue
		}
		handler.addrList = append(handler.addrList, ipAddr)
	}
	handler.addr = net.ParseIP(IPAddrs[0])

	//Read Ports
	scanner.Scan()
	Ports := strings.Split(scanner.Text(), " ")
	temp, _ := strconv.Atoi(Ports[0])
	handler.ServicePortRangeS = uint16(temp)
	temp, _ = strconv.Atoi(Ports[1])
	handler.ServicePortRangeE = uint16(temp)

	handler.portRange = makeRange(handler.ServicePortRangeS, handler.ServicePortRangeE)
	return nil
}

/*
Structure : proxyHandler
Method    : startPMDBClient
Arguments : None
Return(s) : error

Description : Starts PMDB client
*/
func (handler *proxyHandler) startPMDBClient() error {
	var err error

	//Get client object
	handler.pmdbClientObj = pmdbClient.PmdbClientNew((handler.raftUUID.String()), (handler.clientUUID.String()))
	if handler.pmdbClientObj == nil {
		return errors.New("PMDB client object is empty")
	}

	//Start pumicedb client
	err = handler.pmdbClientObj.Start()
	if err != nil {
		return err
	}

	leaderUuid, err := handler.pmdbClientObj.PmdbGetLeader()
	for err != nil {
		leaderUuid, err = handler.pmdbClientObj.PmdbGetLeader()
	}
	log.Info("Leader uuid : ", leaderUuid.String())

	//Store rncui in nkvclientObj
	handler.pmdbClientObj.AppUUID = uuid.NewV4().String()
	return nil

}

/*
Structure : proxyHandler
Method    : startSerfAgent
Arguments : None
Return(s) : error

Description : Starts Serf Agent
*/
func (handler *proxyHandler) startSerfAgent() error {
	//Setup serf logger if passed in cmd line args
	switch handler.serfLogger {
	case "ignore":
		defaultLogger.SetOutput(ioutil.Discard)
	default:
		f, err := os.OpenFile(handler.serfLogger, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
		if err != nil {
			defaultLogger.SetOutput(os.Stderr)
		} else {
			defaultLogger.SetOutput(f)
		}
	}

	//Fill serf agent configuration
	handler.serfAgentObj = serfAgent.SerfAgentHandler{
		Name:              handler.serfAgentName,
		AddrList:          handler.addrList,
		Addr:              net.ParseIP("127.0.0.1"),
		AgentLogger:       defaultLogger.Default(),
		RaftUUID:          handler.raftUUID,
		ServicePortRangeS: handler.ServicePortRangeS,
		ServicePortRangeE: handler.ServicePortRangeE,
		AppType:           "PROXY",
	}
	//Start serf agent
	_, err := handler.serfAgentObj.SerfAgentStartup(true)
	return err
}

/*
Func      : getAnyEntryFromStringMap
Arguments : map[string]map[string]string
Return(s) : map[string]string

Description : A helper func to get a entry from nested string map;
*/
func getAnyEntryFromStringMap(mapSample map[string]map[string]string) map[string]string {
	for _, v := range mapSample {
		return v
	}
	return nil
}

/*
Func      : validateCheckSum
Arguments : map[string]string, string
Return(s) : error

Description : A helper func to validate recieved checksum with checksum of the data(map[string]string).
*/
func validateCheckSum(data map[string]string, checksum string) error {
	keys := make([]string, 0, len(data))
	var allDataArray []string

	//Append map keys to key array
	for k := range data {
		keys = append(keys, k)
	}

	//Sort the key array to ensure uniformity
	sort.Strings(keys)
	//Iterate over the sorted keys and append the value to array
	for _, k := range keys {
		allDataArray = append(allDataArray, k+data[k])
	}

	//Convert value array to byte slice
	byteArray, err := json.Marshal(allDataArray)
	if err != nil {
		return err
	}

	//Calculate checksum for the byte slice
	calculatedChecksum := crc32.ChecksumIEEE(byteArray)

	//Convert the checksum to uint32 and compare with identified checksum
	convertedCheckSum := binary.LittleEndian.Uint32([]byte(checksum))
	if calculatedChecksum != convertedCheckSum {
		return errors.New("Checksum mismatch")
	}
	return nil
}

/*
Structure : proxyHandler
Method    : GetPMDBServerConfig
Arguments : None
Return(s) : error

Description : Get PMDB server configs from serf gossip and store in file. The generated PMDB config
file is used by PMDB client to connet to the PMDB cluster.
*/
func (handler *proxyHandler) GetPMDBServerConfig() error {
	//Iterate till getting PMDB config data from serf gossip
	var allPmdbServerGossip map[string]map[string]string
	for len(allPmdbServerGossip) == 0 {
		allPmdbServerGossip = handler.serfAgentObj.GetTags("Type", "PMDB_SERVER")
		time.Sleep(2 * time.Second)
	}
	log.Info("PMDB config from gossip : ", allPmdbServerGossip)

	var err error
	pmdbServerGossip := getAnyEntryFromStringMap(allPmdbServerGossip)

	//Validate checksum; Get checksum entry from Map and delete that entry
	recvCheckSum := pmdbServerGossip["CS"]
	delete(pmdbServerGossip, "CS")
	err = validateCheckSum(pmdbServerGossip, recvCheckSum)
	if err != nil {
		return err
	}

	//Get Raft UUID from the map
	handler.raftUUID, err = uuid.FromString(pmdbServerGossip["RU"])
	if err != nil {
		log.Error("Error :", err)
		return err
	}

	//Get PMDB config from the map
	handler.PMDBServerConfigByteMap = make(map[string][]byte)
	for key, value := range pmdbServerGossip {
		decompressedUUID, err := compressionLib.DecompressUUID(key)
		if err == nil {
			peerConfig := PumiceDBCommon.PeerConfigData{}
			compressionLib.DecompressStructure(&peerConfig, key+value)
			log.Info("Peer config : ", peerConfig)
			handler.PMDBServerConfigArray = append(handler.PMDBServerConfigArray, peerConfig)
			handler.PMDBServerConfigByteMap[decompressedUUID], _ = json.Marshal(peerConfig)
		}
	}

	log.Info("Decompressed PMDB server config array : ", handler.PMDBServerConfigArray)
	path := os.Getenv("NIOVA_LOCAL_CTL_SVC_DIR")
	//Create PMDB server config dir
	os.Mkdir(path, os.ModePerm)
	return handler.dumpConfigToFile(path + "/")
}

/*
Structure : proxyHandler
Method    : dumpConfigToFile
Arguments : string
Return(s) : error

Description : Dump PMDB server configs from map to file
*/
func (handler *proxyHandler) dumpConfigToFile(outfilepath string) error {
	//Generate .raft
	raft_file, err := os.Create(outfilepath + (handler.raftUUID.String()) + ".raft")
	if err != nil {
		return err
	}

	_, errFile := raft_file.WriteString("RAFT " + (handler.raftUUID.String()) + "\n")
	if errFile != nil {
		return err
	}

	for _, peer := range handler.PMDBServerConfigArray {
		raft_file.WriteString("PEER " + (uuid.UUID(peer.UUID).String()) + "\n")
	}

	raft_file.Sync()
	raft_file.Close()

	//Generate .peer
	for _, peer := range handler.PMDBServerConfigArray {
		peer_file, err := os.Create(outfilepath + (uuid.UUID(peer.UUID).String()) + ".peer")
		if err != nil {
			log.Error(err)
		}

		_, errFile := peer_file.WriteString(
			"RAFT         " + (handler.raftUUID.String()) +
				"\nIPADDR       " + peer.IPAddr.String() +
				"\nPORT         " + strconv.Itoa(int(peer.Port)) +
				"\nCLIENT_PORT  " + strconv.Itoa(int(peer.ClientPort)) +
				"\nSTORE        ./*.raftdb\n")

		if errFile != nil {
			return errFile
		}
		peer_file.Sync()
		peer_file.Close()
	}
	return nil
}

/*
Structure : proxyHandler
Method    : WriteCallBack
Arguments : []byte
Return(s) : error

Description : Call back for PMDB writes requests to HTTP server.
*/
func (handler *proxyHandler) WriteCallBack(request []byte, response *[]byte) error {
	var replySize int64
	requestObj := requestResponseLib.Request{}
	dec := gob.NewDecoder(bytes.NewBuffer(request))
	err := dec.Decode(&requestObj)
	if err != nil {
		return err
	}

	var rncui string
	if(requestObj.RequestType == requestResponseLib.APP_REQ) {
		rncui = requestObj.RequestPayload.(requestResponseLib.KVRequest).Rncui
	} else {
		//TODO: Fix the rnuci
		rncui = requestObj.RequestPayload.(requestResponseLib.LeaseReq).Rncui
	}

	reqArgs := &pmdbClient.PmdbReqArgs {
		Rncui: rncui,
		ReqByteArr: request,
		GetResponse: 0,
		ReplySize: &replySize,
	}

	_, err = handler.pmdbClientObj.WriteEncoded(reqArgs)

	
	var responseObj requestResponseLib.KVResponse
	if err != nil {
		responseObj.Status = 1
	} else {
		responseObj.Status = 0
	}

	var responseBuffer bytes.Buffer
	enc := gob.NewEncoder(&responseBuffer)
	err = enc.Encode(responseObj)
	*response = responseBuffer.Bytes()

	return err
}

/*
Structure : proxyHandler
Method : ReadWrapper
Arguments : key, *[]byte
Return(s) : error

Description : A wrapper for PMDB ReadCallBack
*/
func (handler *proxyHandler) ReadWrapper(key string, response *[]byte) error {
	var baserequest requestResponseLib.Request
	baserequest.RequestType = requestResponseLib.APP_REQ 
	request := requestResponseLib.KVRequest{}
	request.Operation = "read"
	request.Key = key
	baserequest.RequestPayload = request
	var requestBytes bytes.Buffer
	enc := gob.NewEncoder(&requestBytes)
	enc.Encode(baserequest)
	reqArgs := &pmdbClient.PmdbReqArgs {
		Rncui: "",
		ReqByteArr: requestBytes.Bytes(),
		Response: response,
	}
	return handler.pmdbClientObj.ReadEncoded(reqArgs)
}

/*
Structure : proxyHandler
Method    : ReadCallBack
Arguments : []byte, *[]byte
Return(s) : error

Description : Call back for PMDB read requests to HTTP server.
*/
func (handler *proxyHandler) ReadCallBack(request []byte, response *[]byte) error {
	reqArgs := &pmdbClient.PmdbReqArgs {
		Rncui: "",
		ReqByteArr: request,
		Response: response,
	}

	return handler.pmdbClientObj.ReadEncoded(reqArgs)
}

/*
Structure : proxyHandler
Method    : startHTTPServer
Arguments : None
Return(s) : error

Description : Starts HTTP server.
*/
func (handler *proxyHandler) startHTTPServer() error {
	//Start httpserver.
	handler.httpServerObj = httpServer.HTTPServerHandler{
		Addr:             handler.addr,
		PortRange:        handler.portRange,
		PUTHandler:       handler.WriteCallBack,
		GETHandler:       handler.ReadCallBack,
		PMDBServerConfig: handler.PMDBServerConfigByteMap,
		RecvdPort:        &RecvdPort,
		AppType:          "Proxy",
	}
	handler.httpServerObj.HTTPConnectionLimit, _ = strconv.Atoi(handler.limit)
	if handler.requireStat != "0" {
		handler.httpServerObj.StatsRequired = true
	}
	err := handler.httpServerObj.Start_HTTPServer()
	return err
}

/*
Structure : proxyHandler
Method    : setSerfGossipData
Arguments : None
Return(s) : None

Description : Set gossip data for proxy
*/
func (handler *proxyHandler) setSerfGossipData() {
	tag := make(map[string]string)
	//Static tags

	tag["Hport"] = strconv.Itoa(int(handler.httpPort))
	tag["Aport"] = strconv.Itoa(int(handler.serfAgentObj.Aport))
	tag["Rport"] = strconv.Itoa(int(handler.serfAgentObj.RpcPort))
	tag["Type"] = "PROXY"
	handler.serfAgentObj.SetNodeTags(tag)

	//Dynamic tag : Leader UUID of PMDB cluster
	for {
		leader, err := handler.pmdbClientObj.PmdbGetLeader()
		if err != nil {
			log.Error(err)
		} else {
			tag["Leader UUID"] = leader.String()
			handler.serfAgentObj.SetNodeTags(tag)
			log.Trace("(Proxy)", tag)
		}
		//Wait for sometime to pmdb client to establish connection with raft cluster or raft cluster to appoint a leader
		time.Sleep(300 * time.Millisecond)
	}
}

/*
Structure : proxyHandler
Method    : killSignalHandler
Arguments : None
Return(s) : None

Description : Generates HTTP request status file on kill signal
*/
func (handler *proxyHandler) killSignalHandler() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		json_data, _ := json.MarshalIndent(handler.httpServerObj.Stat, "", " ")
		_ = ioutil.WriteFile((handler.clientUUID.String())+".json", json_data, 0644)
		log.Info("(Proxy) Received a kill signal")
		os.Exit(1)
	}()
}

/*
Structure : proxyHandler
Method    : checkHTTPLiveness
Arguments : None
Return(s) : None

Description : Checks status of the http server
*/
func (handler *proxyHandler) checkHTTPLiveness() {
	var emptyByteArray []byte
	for {
		fmt.Println("/check on port - ", RecvdPort)
		_, err := httpClient.HTTP_Request(emptyByteArray, "127.0.0.1:"+strconv.Itoa(int(RecvdPort))+"/check", false)
		if err != nil {
			log.Error("HTTP Liveness - ", err)
			fmt.Println("HTTP Liveness - ", err)
		} else {
			log.Info("HTTP Liveness - HTTP Server is alive")
			fmt.Println("HTTP Liveness - Server is alive")
			handler.httpPort = uint16(RecvdPort)
			break
		}
		time.Sleep(1 * time.Second)
	}
}

func main() {

	var err error
	var i int

	proxyObj := proxyHandler{}
	//Get commandline paraameters.
	proxyObj.getCmdParams()

	flag.Usage = usage
	flag.Parse()
	if flag.NFlag() == 0 {
		usage()
		os.Exit(-1)
	}
	//niovaServer.clientUUID = uuid.NewV4().String()
	//Create log file
	err = PumiceDBCommon.InitLogger(proxyObj.logPath)
	switch proxyObj.logLevel {
	case "Info":
		log.SetLevel(log.InfoLevel)
	case "Trace":
		log.SetLevel(log.TraceLevel)
	}
	if err != nil {
		log.Error("(Proxy) Logger error : ", err)
	}

	//Apply config
	err = proxyObj.getConfigData()
	if err != nil {
		log.Error("(Proxy) Error while getting config data : ", err)
		os.Exit(1)
	}

	//Start serf agent handler
	log.Info("Starting serf agent handler")
	err = proxyObj.startSerfAgent()
	if err != nil {
		log.Error("Error while starting Serf Agent")
	}

	//Get PMDB server config data
	err = proxyObj.GetPMDBServerConfig()
	if err != nil {
		log.Error("Could not get PMDB Server config data : ", err)
		os.Exit(1)
	}

	err = proxyObj.startPMDBClient()
	if err != nil {
		log.Error("(Niovakv Server) Error while starting pmdb client : ", err)
		os.Exit(1)
	}

	//Start http server
	go func() {
		log.Info("Starting HTTP server")
		proxyObj.httpPort = proxyObj.portRange[i]
		err = proxyObj.startHTTPServer()
	}()

	//Stat maker
	if proxyObj.requireStat != "0" {
		go proxyObj.killSignalHandler()
	}

	//Wait till http server is up and running
	proxyObj.checkHTTPLiveness()

	//Start the gossip
	proxyObj.setSerfGossipData()
}
