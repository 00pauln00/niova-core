package main

import (
	"bufio"
	"bytes"
	"common/httpClient"
	"common/httpServer"
	"common/requestResponseLib"
	"common/serfAgent"
	"encoding/gob"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	defaultLogger "log"
	"net"
	pmdbClient "niova/go-pumicedb-lib/client"
	PumiceDBCommon "niova/go-pumicedb-lib/common"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	uuid "github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
)

type proxyHandler struct {
	//Other
	configPath string
	logLevel   string

	//Pmdb nivoa client
	raftUUID                uuid.UUID
	clientUUID              uuid.UUID
	logPath                 string
	PMDBServerConfigArray   []PeerConfigData
	PMDBServerConfigByteMap map[string][]byte
	pmdbClientObj           *pmdbClient.PmdbClientObj

	//Service port range
	ServicePortRangeS uint16
	ServicePortRangeE uint16
	addr              net.IP
	addrList          []net.IP
	portRange         []uint16

	//Serf agent
	serfAgentName string
	serfLogger    string
	serfAgentObj  serfAgent.SerfAgentHandler

	//Http
	limit         string
	requireStat   string
	httpServerObj httpServer.HTTPServerHandler
	httpPort      uint16
}

var RecvdPort int

type PeerConfigData struct {
	PeerUUID   string
	ClientPort string
	Port       string
	IPAddr     string
}

func usage() {
	flag.PrintDefaults()
	os.Exit(0)
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

//Function to get command line arguments
func (handler *proxyHandler) getCmdLineArgs() {
	var tempRaftUUID, tempClientUUID string

	//Prepare default logpath
	defaultLogPath := "/" + "tmp" + "/" + "niovaKVServer" + ".log"
	flag.StringVar(&tempRaftUUID, "r", "NULL", "raft uuid")
	flag.StringVar(&tempClientUUID, "u", uuid.NewV4().String(), "client uuid")
	flag.StringVar(&handler.logPath, "l", defaultLogPath, "log filepath")
	flag.StringVar(&handler.configPath, "c", "./", "serf config path")
	flag.StringVar(&handler.serfAgentName, "n", "NULL", "serf agent name")
	flag.StringVar(&handler.limit, "e", "500", "No of concurrent request")
	flag.StringVar(&handler.serfLogger, "sl", "ignore", "serf logger file [default:ignore]")
	flag.StringVar(&handler.logLevel, "ll", "", "Set log level for the execution")
	flag.StringVar(&handler.requireStat, "s", "0", "If required server stat about request enter 1")
	flag.Parse()

	handler.raftUUID, _ = uuid.FromString(tempRaftUUID)
	handler.clientUUID, _ = uuid.FromString(tempClientUUID)
}

/*
Config should contain following:
IP_Addrs of other proxies with space seperated
Start_of_port_range End_of_port_range
*/

/*
Function name : getProxyConfigData

Description :
Parses the config file and get the proxy's configutation data
and other serf agent address to join in the gossip mesh

Parameters : nil

Return : error
*/
func (handler *proxyHandler) getProxyConfigData() error {
	if _, err := os.Stat(handler.configPath); os.IsNotExist(err) {
		return err
	}
	reader, err := os.OpenFile(handler.configPath, os.O_RDONLY, 0444)
	if err != nil {
		return err
	}

	//IPAddrs
	scanner := bufio.NewScanner(reader)
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
	handler.addr = net.ParseIP("127.0.0.1")

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
Function name : startPMDBClient

Description : Initialize PMDB Client

Parameters : nil

Return : error
*/
func (handler *proxyHandler) startPMDBClient() error {
	var err error

	//Get client object.
	handler.pmdbClientObj = pmdbClient.PmdbClientNew(handler.raftUUID.String(), handler.clientUUID.String())
	if handler.pmdbClientObj == nil {
		return errors.New("PMDB client object is empty")
	}

	//Start pumicedb client.
	err = handler.pmdbClientObj.Start()
	if err != nil {
		return err
	}

	//Store rncui in nkvclientObj.i
	handler.pmdbClientObj.AppUUID = uuid.NewV4().String()
	return nil

}

//Start the serf agent
func (handler *proxyHandler) start_SerfAgent() error {
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

	handler.serfAgentObj = serfAgent.SerfAgentHandler{}
	handler.serfAgentObj.Name = handler.serfAgentName
	handler.serfAgentObj.AgentLogger = defaultLogger.Default()
	handler.serfAgentObj.AddrList = handler.addrList
	handler.serfAgentObj.Addr = handler.addr
	handler.serfAgentObj.ServicePortRangeS = handler.ServicePortRangeS
	handler.serfAgentObj.ServicePortRangeE = handler.ServicePortRangeE
	handler.serfAgentObj.RaftUUID = handler.raftUUID
	handler.serfAgentObj.AppType = "PROXY"

	//Start serf agent
	_, err := handler.serfAgentObj.SerfAgentStartup(true)

	return err
}

//Write callback definition for HTTP server
func (handler *proxyHandler) WriteCallBack(request []byte, response *[]byte) error {
	idq := atomic.AddUint64(&handler.pmdbClientObj.WriteSeqNo, uint64(1))
	rncui := fmt.Sprintf("%s:0:0:0:%d", handler.pmdbClientObj.AppUUID, idq)
	var replySize int64

	_, err := handler.pmdbClientObj.WriteEncoded(request, rncui, 0, &replySize)
	if err != nil {
		responseObj := requestResponseLib.KVResponse{
			Status: 1,
		}
		var responseBuffer bytes.Buffer
		enc := gob.NewEncoder(&responseBuffer)
		err = enc.Encode(responseObj)
		*response = responseBuffer.Bytes()
	}
	return err
}

//Read call definition for HTTP server
func (handler *proxyHandler) ReadCallBack(request []byte, response *[]byte) error {
	return handler.pmdbClientObj.ReadEncoded(request, "", response)
}

func (handler *proxyHandler) start_HTTPServer() error {
	//Start httpserver
	handler.httpServerObj = httpServer.HTTPServerHandler{}
	handler.httpServerObj.Addr = handler.addr
	handler.httpServerObj.PortRange = handler.portRange
	handler.httpServerObj.PUTHandler = handler.WriteCallBack
	handler.httpServerObj.GETHandler = handler.ReadCallBack
	handler.httpServerObj.HTTPConnectionLimit, _ = strconv.Atoi(handler.limit)
	handler.httpServerObj.PMDBServerConfig = handler.PMDBServerConfigByteMap
	handler.httpServerObj.RecvdPort = &RecvdPort
	handler.httpServerObj.AppType = "Proxy"
	if handler.requireStat != "0" {
		handler.httpServerObj.StatsRequired = true
	}
	err := handler.httpServerObj.Start_HTTPServer()
	return err
}

//Get gossip data
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

func (handler *proxyHandler) killSignal_Handler() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		json_data, _ := json.MarshalIndent(handler.httpServerObj.Stat, "", " ")
		_ = ioutil.WriteFile(handler.clientUUID.String()+".json", json_data, 0644)
		log.Info("(Proxy) Received a kill signal")
		os.Exit(1)
	}()
}

//Main func
func main() {

	var err error

	proxyObj := proxyHandler{}
	//Get commandline paraameters.
	proxyObj.getCmdLineArgs()

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

	//get config data
	err = proxyObj.getProxyConfigData()
	if err != nil {
		log.Error("(Proxy) Error while getting config data : ", err)
		os.Exit(1)
	}

	//Start serf agent handler
	err = proxyObj.start_SerfAgent()
	if err != nil {
		log.Error("Error while starting serf agent : ", err)
		os.Exit(1)
	}

	//Create a niovaKVServerHandler
	err = proxyObj.startPMDBClient()
	if err != nil {
		log.Error("(Niovakv Server) Error while starting pmdb client : ", err)
		os.Exit(1)
	}

	//Start http server
	go func() {
		log.Info("Starting HTTP server")
		err = proxyObj.start_HTTPServer()
		if err != nil {
			log.Error("Error while starting http server : ", err)
		}
	}()

	//Stat maker
	if proxyObj.requireStat != "0" {
		go proxyObj.killSignal_Handler()
	}
	//Wait till http server is up and running
	proxyObj.checkHTTPLiveness()

	//Start the gossip
	proxyObj.setSerfGossipData()
}
