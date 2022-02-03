package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	uuid "github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	defaultLogger "log"
	pmdbClient "niova/go-pumicedb-lib/client"
	"niova/go-pumicedb-lib/common"
	"common/httpServer"
	//"common/pmdbClient"
	"common/serfAgent"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type proxyHandler struct {
	//Other
	configPath string
	logLevel   string
	
	//Niovakvserver
	addr string

	//Pmdb nivoa client
	raftUUID                string
	clientUUID              string
	logPath                 string
	PMDBServerConfigArray   []PeerConfigData
	PMDBServerConfigByteMap map[string][]byte
	pmdbClientObj           *pmdbClient.PmdbClientObj

	//Serf agent
	serfAgentName         string
	serfAgentPort         string
	serfAgentRPCPort      string
	serfPeersFilePath     string
	serfLogger            string
	serfAgentObj          serfAgent.SerfAgentHandler

	//Http
	httpPort      string
	limit         string
	requireStat   string
	httpServerObj httpServer.HTTPServerHandler
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

//Function to get command line parameters while starting of the client.
func (handler *proxyHandler) get_CmdParams() {
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
	flag.StringVar(&handler.serfPeersFilePath, "pa", "NULL", "Path to pmdb server gossip addrs")
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
func (handler *proxyHandler) get_ConfigData() error {
	reader, err := os.Open(handler.configPath)
	if err != nil {
		return err
	}
	filescanner := bufio.NewScanner(reader)
	filescanner.Split(bufio.ScanLines)
	for filescanner.Scan() {
		input := strings.Split(filescanner.Text(), " ")
		if input[0] == handler.serfAgentName {
			handler.addr = input[1]
			handler.serfAgentPort = input[2]
			handler.serfAgentRPCPort = input[3]
			handler.httpPort = input[4]
		}
	}
	if handler.serfAgentPort == "" {
		return errors.New("Agent name not matching or not provided")
	}
	return nil
}

//start the Niovakvpmdbclient
func (handler *proxyHandler) start_PMDBClient() error {
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

//start the SerfAgentHandler
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
	joinAddrs, err := serfAgent.Get_PeerAddress(handler.serfPeersFilePath)
	if err != nil {
		return err
	}
	//Start serf agent
	_, err = handler.serfAgentObj.Serf_agent_startup(joinAddrs, true)

	return err
}

func (handler *proxyHandler) get_PMDBServer_Config() {
	var raftUUID, peerConfig string
	for raftUUID == "" {
		peerConfig, raftUUID = handler.serfAgentObj.Get_tags()
		time.Sleep(2 * time.Second)
	}
	log.Info("PMDB config recvd from gossip : ", peerConfig)
	handler.raftUUID = raftUUID
	handler.PMDBServerConfigByteMap = make(map[string][]byte)

	splitData := strings.Split(peerConfig, "/")
	var PeerUUID, ClientPort, Port, IPAddr string
	flag := false
	for i, element := range splitData {
		switch i % 4 {
		case 0:
			PeerUUID = element
		case 1:
			IPAddr = element
		case 2:
			ClientPort = element
		case 3:
			flag = true
			Port = element
		}
		if flag {
			peerConfig := PeerConfigData{
				PeerUUID:   PeerUUID,
				IPAddr:     IPAddr,
				Port:       Port,
				ClientPort: ClientPort,
			}
			handler.PMDBServerConfigArray = append(handler.PMDBServerConfigArray, peerConfig)
			handler.PMDBServerConfigByteMap[PeerUUID], _ = json.Marshal(peerConfig)
			flag = false
		}
	}
	path := os.Getenv("NIOVA_LOCAL_CTL_SVC_DIR")
	//os.Mkdir(path+"/"+handler.clientUUID,os.ModePerm)
	//log.Info("Peer UUID : ",handler.clientUUID)
	//path += "/" + handler.clientUUID
	os.Mkdir(path, os.ModePerm)
	//path += "/PMDBConfig/"
	//log.Info("Altered NIOVA_LOCAL_CTL_SVC_DIR is ", path)
	//os.Setenv("NIOVA_LOCAL_CTL_SVC_DIR",path)
	handler.dump_ConfigToFile(path+"/")
}

func (handler *proxyHandler) dump_ConfigToFile(outfilepath string) {
	//Generate .raft
	raft_file, err := os.Create(outfilepath + handler.raftUUID + ".raft")
	if err != nil {
		log.Error(err)
	}

	_, errFile := raft_file.WriteString("RAFT " + handler.raftUUID + "\n")
	if errFile != nil {
		log.Error(errFile)
	}

	for _, peer := range handler.PMDBServerConfigArray {
		raft_file.WriteString("PEER " + peer.PeerUUID + "\n")
	}

	raft_file.Sync()
	raft_file.Close()

	//Generate .peer
	for _, peer := range handler.PMDBServerConfigArray {
		peer_file, err := os.Create(outfilepath + peer.PeerUUID + ".peer")
		if err != nil {
			log.Error(err)
		}

		_, errFile := peer_file.WriteString(
			"RAFT         " + handler.raftUUID +
				"\nIPADDR       " + peer.IPAddr +
				"\nPORT         " + peer.Port +
				"\nCLIENT_PORT  " + peer.ClientPort +
				"\nSTORE        ./*.raftdb\n")

		if errFile != nil {
			log.Error(errFile)
		}
		peer_file.Sync()
		peer_file.Close()
	}
}

func (handler *proxyHandler) start_HTTPServer() error {
	//Start httpserver.
	handler.httpServerObj = httpServer.HTTPServerHandler{}
	handler.httpServerObj.Addr = handler.addr
	handler.httpServerObj.Port = handler.httpPort
	handler.httpServerObj.PMDBClientHandlerObj = handler.pmdbClientObj
	handler.httpServerObj.HTTPConnectionLimit, _ = strconv.Atoi(handler.limit)
	handler.httpServerObj.PMDBServerConfig = handler.PMDBServerConfigByteMap
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
	tag["Aport"] = handler.serfAgentPort
	tag["Rport"] = handler.serfAgentRPCPort
	tag["Type"]  = "PROXY"
	handler.serfAgentObj.Set_node_tags(tag)
	for {
		leader, err := handler.pmdbClientObj.PmdbGetLeader()
		if err != nil {
			log.Error(err)
			//Wait for sometime to pmdb client to establish connection with raft cluster or raft cluster to appoint a leader
			time.Sleep(5 * time.Second)
			continue
		}
		tag["Leader UUID"] = leader.String()
		handler.serfAgentObj.Set_node_tags(tag)
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
	proxyObj.get_CmdParams()

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
	err = proxyObj.get_ConfigData()
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

	//Get PMDB server config data
	proxyObj.get_PMDBServer_Config()

	//Create a niovaKVServerHandler
	err = proxyObj.start_PMDBClient()
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
