package main

import (
	"bufio"
	"errors"
	"flag"
	"io/ioutil"
	PumiceDBCommon "niova/go-pumicedb-lib/common"
	"niovakv/httpserver"
	"os"
	"strconv"
	"strings"
	"time"
	"encoding/json"
	"os/signal"
	"syscall"
	defaultLogger "log"
	"niovakv/niovakvpmdbclient"
	"niovakvserver/serfagenthandler"
	log "github.com/sirupsen/logrus"
	uuid "github.com/satori/go.uuid"
)

type niovaKVServerHandler struct {
	//Other
	configPath string
	logLevel   string
	//Niovakvserver
	addr string

	//Pmdb nivoa client
	raftUUID     string
	clientUUID   string
	logPath      string
	pmdbClient   *niovakvpmdbclient.NiovaKVClient

	//Serf agent
	agentName           string
	agentPort           string
	agentRPCPort        string
	peerAddrsFilePath   string
	serfLogger          string
	serfAgentHandler    serfagenthandler.SerfAgentHandler

	//Http
	httpPort       string
	limit          string
	requireStat    string
	HttpHandler    httpserver.HttpServerHandler
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
	flag.StringVar(&handler.clientUUID, "u", "NULL", "client uuid")
	flag.StringVar(&handler.logPath, "l", defaultLogPath, "log filepath")
	flag.StringVar(&handler.configPath, "c", "./", "serf config path")
	flag.StringVar(&handler.agentName, "n", "NULL", "serf agent name")
	flag.StringVar(&handler.limit, "e", "500", "No of concurrent request")
	flag.StringVar(&handler.serfLogger, "sl", "ignore", "serf logger file [default:ignore]")
	flag.StringVar(&handler.logLevel, "ll", "", "Set log level for the execution")
	flag.StringVar(&handler.requireStat , "s","0","If required server stat about request enter 1")
	flag.StringVar(&handler.peerAddrsFilePath , "pa", "NULL", "Path to pmdb server gossip addrs")
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

func (handler *niovaKVServerHandler) getConfigData_Gossip() {
	Data := handler.serfAgentHandler.GetTags()
	log.Info(Data)
	JsonString, _ := json.MarshalIndent(Data,""," ")
	 _ = ioutil.WriteFile("config.json", JsonString, 0644)
}

func (handler *niovaKVServerHandler) startHTTPServer() error {
	//Start httpserver.
	handler.HttpHandler = httpserver.HttpServerHandler{}
	handler.HttpHandler.Addr = handler.addr
	handler.HttpHandler.Port = handler.httpPort
	handler.HttpHandler.NKVCliObj = handler.pmdbClient
	handler.HttpHandler.Limit, _ = strconv.Atoi(handler.limit)
	if handler.requireStat != "0" {
		handler.HttpHandler.NeedStats = true
	}
	err := handler.HttpHandler.StartServer()
	return err
}

//Get gossip data
func (handler *niovaKVServerHandler) getGossipData() {
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
	sigs := make(chan os.Signal,1)
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

	//Create log file.
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
		log.Panic("(Niovakv Server) Error while getting config data : ", err)
		os.Exit(1)
	}
	
	//Start serf agent handler
        err = niovaServer.startSerfAgent()
        if err != nil {
                log.Panic("Error while starting serf agent : ", err)
                os.Exit(1)
        }


	niovaServer.getConfigData_Gossip()

	//Create a niovaKVServerHandler
	err = niovaServer.startPMDBclient()
	if err != nil {
		log.Panic("(Niovakv Server) Error while starting pmdb client : ", err)
		os.Exit(1)
	}

	//Start http server
	go func(){
		err = niovaServer.startHTTPServer()
		if err != nil {
			log.Panic("Error while starting http server : ", err)
		}
	}()

	//Stat maker
	if niovaServer.requireStat != "0" {
		go niovaServer.killSignalHandler()
	}


	//Start the gossip
	niovaServer.getGossipData()
}
