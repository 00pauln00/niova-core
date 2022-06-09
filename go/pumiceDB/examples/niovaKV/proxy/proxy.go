package main

import (
	"bufio"
	"common/httpServer"
	"common/serfAgent"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	defaultLogger "log"
	"net"
	pmdbClient "niova/go-pumicedb-lib/client"
	"niova/go-pumicedb-lib/common"
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

	//Proxy
	addr net.IP

	//Pmdb nivoa client
	raftUUID                string
	clientUUID              string
	logPath                 string
	PMDBServerConfigArray   []PeerConfigData
	PMDBServerConfigByteMap map[string][]byte
	pmdbClientObj           *pmdbClient.PmdbClientObj

	//Serf agent
	serfAgentName    string
	serfAgentPort    uint16
	serfAgentRPCPort uint16
	serfLogger       string
	serfAgentObj     serfAgent.SerfAgentHandler

	//Http
	httpPort      string
	limit         string
	requireStat   string
	httpServerObj httpServer.HTTPServerHandler
	buckets		  int64
}

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

//Function to get command line arguments
func (handler *proxyHandler) getCmdLineArgs() {
	//Prepare default logpath
	defaultLogPath := "/" + "tmp" + "/" + "niovaKVServer" + ".log"
	flag.StringVar(&handler.raftUUID, "r", "NULL", "raft uuid")
	flag.StringVar(&handler.clientUUID, "u", uuid.NewV4().String(), "client uuid")
	flag.StringVar(&handler.logPath, "l", defaultLogPath, "log filepath")
	flag.StringVar(&handler.configPath, "c", "./", "serf config path")
	flag.StringVar(&handler.serfAgentName, "n", "NULL", "serf agent name")
	flag.StringVar(&handler.limit, "e", "500", "No of concurrent request")
	flag.StringVar(&handler.serfLogger, "sl", "ignore", "serf logger file [default:ignore]")
	flag.StringVar(&handler.logLevel, "ll", "", "Set log level for the execution")
	flag.StringVar(&handler.requireStat, "s", "0", "If required server stat about request enter 1")
	flag.Int64Var(&handler.buckets, "b", 24, "If required server stat, number of buckets in historgam")
	flag.Parse()
}

/*
Config should contain following:
Name, Addr, Aport, Rport, Hport

Name //For serf agent name, must be unique for each node
Addr //Addr for serf agent and http listening
Aport //Serf agent-agent communication
Rport //Serf agent-client communication
Hport //Http listener port
*/

/*
Function name : getProxyConfigData

Description :
Parses the config file and get the proxy's configutation data
and other serf agent address to join in the gossip mesh

Config should contain following:
Name, Addr, Aport, Rport, Hport
Name //For serf agent name, must be unique for each node
Addr //Addr for serf agent and http listening
Aport //Serf agent-agent communication
Rport //Serf agent-client communication
Hport //Http listener port

Parameters : nil

Return : error
*/
func (handler *proxyHandler) getProxyConfigData() error {
	reader, err := os.Open(handler.configPath)
	if err != nil {
		return err
	}
	filescanner := bufio.NewScanner(reader)
	filescanner.Split(bufio.ScanLines)
	var flag bool
	for filescanner.Scan() {
		input := strings.Split(filescanner.Text(), " ")
		if input[0] == handler.serfAgentName {
			handler.addr = net.ParseIP(input[1])
			aport := input[2]
			buffer, err := strconv.ParseUint(aport, 10, 16)
			handler.serfAgentPort = uint16(buffer)
			if err != nil {
				return errors.New("Agent port is out of range")
			}

			rport := input[3]
			buffer, err = strconv.ParseUint(rport, 10, 16)
			if err != nil {
				return errors.New("Agent port is out of range")
			}

			handler.serfAgentRPCPort = uint16(buffer)
			handler.httpPort = input[4]
			flag = true
		}
	}
	if !flag {
		return errors.New("Agent name not matching or not provided")
	}
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
	handler.pmdbClientObj = pmdbClient.PmdbClientNew(handler.raftUUID, handler.clientUUID)
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
	handler.serfAgentObj.BindAddr = handler.addr
	handler.serfAgentObj.BindPort = handler.serfAgentPort
	handler.serfAgentObj.AgentLogger = defaultLogger.Default()
	handler.serfAgentObj.RpcAddr = handler.addr
	handler.serfAgentObj.RpcPort = handler.serfAgentRPCPort
	joinAddrs, err := serfAgent.GetPeerAddress(handler.configPath)
	if err != nil {
		return err
	}
	//Start serf agent
	_, err = handler.serfAgentObj.SerfAgentStartup(joinAddrs, true)

	return err
}

//Write callback definition for HTTP server
func (handler *proxyHandler) WriteCallBack(request []byte) error {
	idq := atomic.AddUint64(&handler.pmdbClientObj.WriteSeqNo, uint64(1))
	rncui := fmt.Sprintf("%s:0:0:0:%d", handler.pmdbClientObj.AppUUID, idq)
	return handler.pmdbClientObj.WriteEncoded(request, rncui)
}

//Read call definition for HTTP server
func (handler *proxyHandler) ReadCallBack(request []byte, response *[]byte) error {
	return handler.pmdbClientObj.ReadEncoded(request, "", response)
}

func (handler *proxyHandler) start_HTTPServer() error {
	//Start httpserver
	handler.httpServerObj = httpServer.HTTPServerHandler{}
	handler.httpServerObj.Addr = handler.addr
	handler.httpServerObj.Port = handler.httpPort
	handler.httpServerObj.PUTHandler = handler.WriteCallBack
	handler.httpServerObj.GETHandler = handler.ReadCallBack
	handler.httpServerObj.HTTPConnectionLimit, _ = strconv.Atoi(handler.limit)
	handler.httpServerObj.PMDBServerConfig = handler.PMDBServerConfigByteMap
	handler.httpServerObj.Buckets = handler.buckets
	if handler.requireStat != "0" {
		handler.httpServerObj.StatsRequired = true
	}
	err := handler.httpServerObj.Start_HTTPServer()
	return err
}

//Get gossip data
func (handler *proxyHandler) set_Serf_GossipData() {
	tag := make(map[string]string)
	tag["Hport"] = handler.httpPort
	tag["Aport"] = strconv.Itoa(int(handler.serfAgentPort))
	tag["Rport"] = strconv.Itoa(int(handler.serfAgentRPCPort))
	tag["Type"] = "PROXY"
	handler.serfAgentObj.SetNodeTags(tag)
	for {
		leader, err := handler.pmdbClientObj.PmdbGetLeader()
		if err != nil {
			log.Error(err)
			//Wait for sometime to pmdb client to establish connection with raft cluster or raft cluster to appoint a leader
			time.Sleep(5 * time.Second)
			continue
		}
		tag["Leader UUID"] = leader.String()
		handler.serfAgentObj.SetNodeTags(tag)
		log.Trace("(Proxy)", tag)
		time.Sleep(300 * time.Millisecond)
	}
}

func (handler *proxyHandler) killSignal_Handler() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		json_data, _ := json.MarshalIndent(handler.httpServerObj.Stat, "", " ")
		_ = ioutil.WriteFile(handler.clientUUID+".json", json_data, 0644)
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

	//Start the gossip
	proxyObj.set_Serf_GossipData()
}
