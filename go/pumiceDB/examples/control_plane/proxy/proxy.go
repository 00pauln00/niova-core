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
	PumiceDBCommon "niova/go-pumicedb-lib/common"
	"niovakv/httpserver"
	"niovakv/niovakvpmdbclient"
	"niovakvserver/serfagenthandler"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type niovaKVServerHandler struct {
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
	pmdbClient              *niovakvpmdbclient.NiovaKVClient

	//Serf agent
	agentName         string
	agentPort         string
	agentRPCPort      string
	peerAddrsFilePath string
	serfLogger        string
	serfAgentHandler  serfagenthandler.SerfAgentHandler

	//Http
	httpPort    string
	limit       string
	requireStat string
	HttpHandler httpserver.HttpServerHandler
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
func (handler *niovaKVServerHandler) getCmdParams() {
	//Prepare default logpath
	defaultLogPath := "/" + "tmp" + "/" + "niovaKVServer" + ".log"
	flag.StringVar(&handler.raftUUID, "r", "NULL", "raft uuid")
	flag.StringVar(&handler.clientUUID, "u", uuid.NewV4().String(), "client uuid")
	flag.StringVar(&handler.logPath, "l", defaultLogPath, "log filepath")
	flag.StringVar(&handler.configPath, "c", "./", "serf config path")
	flag.StringVar(&handler.agentName, "n", "NULL", "serf agent name")
	flag.StringVar(&handler.limit, "e", "500", "No of concurrent request")
	flag.StringVar(&handler.serfLogger, "sl", "ignore", "serf logger file [default:ignore]")
	flag.StringVar(&handler.logLevel, "ll", "", "Set log level for the execution")
	flag.StringVar(&handler.requireStat, "s", "0", "If required server stat about request enter 1")
	flag.StringVar(&handler.peerAddrsFilePath, "pa", "NULL", "Path to pmdb server gossip addrs")
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
func (handler *niovaKVServerHandler) getConfigData() error {
	reader, err := os.Open(handler.configPath)
	if err != nil {
		return err
	}
	filescanner := bufio.NewScanner(reader)
	filescanner.Split(bufio.ScanLines)
	for filescanner.Scan() {
		input := strings.Split(filescanner.Text(), " ")
		if input[0] == handler.agentName {
			handler.addr = input[1]
			handler.agentPort = input[2]
			handler.agentRPCPort = input[3]
			handler.httpPort = input[4]
		}
	}
	if handler.agentPort == "" {
		return errors.New("Agent name not matching or not provided")
	}
	return nil
}

//start the Niovakvpmdbclient
func (handler *niovaKVServerHandler) startPMDBclient() error {
	var err error

	//Get client object.
	handler.pmdbClient = niovakvpmdbclient.GetNiovaKVClientObj(handler.raftUUID, handler.clientUUID, handler.logPath)
	if handler.pmdbClient == nil {
		return errors.New("PMDB client object is empty")
	}

	//Start pumicedb client.
	err = handler.pmdbClient.ClientObj.Start()
	if err != nil {
		return err
	}

	//Store rncui in nkvclientObj.
	handler.pmdbClient.AppUuid = uuid.NewV4().String()
	return nil

}

//start the SerfAgentHandler
func (handler *niovaKVServerHandler) startSerfAgent() error {
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

	handler.serfAgentHandler = serfagenthandler.SerfAgentHandler{}
	handler.serfAgentHandler.Name = handler.agentName
	handler.serfAgentHandler.BindAddr = handler.addr
	handler.serfAgentHandler.BindPort, _ = strconv.Atoi(handler.agentPort)
	handler.serfAgentHandler.AgentLogger = defaultLogger.Default()
	handler.serfAgentHandler.RpcAddr = handler.addr
	handler.serfAgentHandler.RpcPort = handler.agentRPCPort
	joinAddrs, err := serfagenthandler.GetPeerAddress(handler.peerAddrsFilePath)
	if err != nil {
		return err
	}
	//Start serf agent
	_, err = handler.serfAgentHandler.Startup(joinAddrs, true)

	return err
}

func (handler *niovaKVServerHandler) getPMDBServerConfigData() {
	var raftUUID, peerConfig string
	for raftUUID == "" {
		peerConfig, raftUUID = handler.serfAgentHandler.GetPMDBServerConfig()
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
	handler.dumpConfigToFile(path+"/")
}

func (handler *niovaKVServerHandler) dumpConfigToFile(outfilepath string) {
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
				"\nSTORE        /home/sshivkumar/configs/e3658ee4-eba6-11eb-853e-9b8cfb7c3b6b/raftdb/e3e86a1c-eba6-11eb-a887-63a2043653ba.raftdb\n")

		if errFile != nil {
			log.Error(errFile)
		}
		peer_file.Sync()
		peer_file.Close()
	}
}

func (handler *niovaKVServerHandler) startHTTPServer() error {
	//Start httpserver.
	handler.HttpHandler = httpserver.HttpServerHandler{}
	handler.HttpHandler.Addr = handler.addr
	handler.HttpHandler.Port = handler.httpPort
	handler.HttpHandler.NKVCliObj = handler.pmdbClient
	handler.HttpHandler.Limit, _ = strconv.Atoi(handler.limit)
	handler.HttpHandler.PMDBServerConfig = handler.PMDBServerConfigByteMap
	if handler.requireStat != "0" {
		handler.HttpHandler.NeedStats = true
	}
	err := handler.HttpHandler.StartServer()
	return err
}

//Get gossip data
func (handler *niovaKVServerHandler) setGossipData() {
	tag := make(map[string]string)
	tag["Hport"] = handler.httpPort
	tag["Aport"] = handler.agentPort
	tag["Rport"] = handler.agentRPCPort
	handler.serfAgentHandler.SetTags(tag)
	for {
		leader, err := handler.pmdbClient.ClientObj.PmdbGetLeader()
		if err != nil {
			log.Error(err)
			//Wait for sometime to pmdb client to establish connection with raft cluster or raft cluster to appoint a leader
			time.Sleep(5 * time.Second)
			continue
		}
		tag["Leader UUID"] = leader.String()
		handler.serfAgentHandler.SetTags(tag)
		log.Trace("(Niovakv Server)", tag)
		time.Sleep(300 * time.Millisecond)
	}
}

func (handler *niovaKVServerHandler) killSignalHandler() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		json_data, _ := json.MarshalIndent(handler.HttpHandler.Stat, "", " ")
		_ = ioutil.WriteFile(handler.clientUUID+".json", json_data, 0644)
		log.Info("(NIOVAKV SERVER) Received a kill signal")
		os.Exit(1)
	}()
}

//Main func
func main() {

	var err error

	niovaServer := niovaKVServerHandler{}
	//Get commandline paraameters.
	niovaServer.getCmdParams()

	flag.Usage = usage
	flag.Parse()
	if flag.NFlag() == 0 {
		usage()
		os.Exit(-1)
	}
	//niovaServer.clientUUID = uuid.NewV4().String()
	//Create log file
	err = PumiceDBCommon.InitLogger(niovaServer.logPath)
	switch niovaServer.logLevel {
	case "Info":
		log.SetLevel(log.InfoLevel)
	case "Trace":
		log.SetLevel(log.TraceLevel)
	}
	if err != nil {
		log.Error("(Niovakv Server) Logger error : ", err)
	}

	//get config data
	err = niovaServer.getConfigData()
	if err != nil {
		log.Error("(Niovakv Server) Error while getting config data : ", err)
		os.Exit(1)
	}

	//Start serf agent handler
	err = niovaServer.startSerfAgent()
	if err != nil {
		log.Error("Error while starting serf agent : ", err)
		os.Exit(1)
	}

	//Get PMDB server config data
	niovaServer.getPMDBServerConfigData()

	//Create a niovaKVServerHandler
	err = niovaServer.startPMDBclient()
	if err != nil {
		log.Error("(Niovakv Server) Error while starting pmdb client : ", err)
		os.Exit(1)
	}

	//Start http server
	go func() {
		log.Info("Starting HTTP server")
		err = niovaServer.startHTTPServer()
		if err != nil {
			log.Error("Error while starting http server : ", err)
		}
	}()

	//Stat maker
	if niovaServer.requireStat != "0" {
		go niovaServer.killSignalHandler()
	}

	//Start the gossip
	niovaServer.setGossipData()
}
