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
	nkvClientObj *niovakvpmdbclient.NiovaKVClient

	//Serf agent
	agentName           string
	agentPort           string
	agentRPCPort        string
	agentJoinAddrs      []string
	serfLogger          string
	serfAgentHandlerObj serfagenthandler.SerfAgentHandler

	//Http
	httpPort       string
	limit          string
	needStats      string
	httpHandlerObj httpserver.HttpServerHandler
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
	flag.StringVar(&handler.needStats, "s", "0", "If need to get request stat")
	flag.StringVar(&handler.serfLogger, "sl", "ignore", "serf logger file [default:ignore]")
	flag.StringVar(&handler.logLevel, "ll", "", "Set log level for the execution")
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
		} else {
			handler.agentJoinAddrs = append(handler.agentJoinAddrs, input[1]+":"+input[2])
		}
	}
	if handler.agentPort == "" {
		return errors.New("Agent name not matching or not provided")
	}
	return nil
}

//start the Niovakvpmdbclient
func (handler *niovaKVServerHandler) startNiovakvpmdbclient() error {
	var err error
	//Get client object.
	handler.nkvClientObj = niovakvpmdbclient.GetNiovaKVClientObj(handler.raftUUID, handler.clientUUID, handler.logPath)
	if handler.nkvClientObj == nil {
		return errors.New("PMDB client object is empty")
	}
	//Start pumicedb client.
	err = handler.nkvClientObj.ClientObj.Start()
	if err != nil {
		return err
	}
	//Store rncui in nkvclientObj.
	handler.nkvClientObj.AppUuid = uuid.NewV4().String()
	return nil

}

//start the SerfAgentHandler
func (handler *niovaKVServerHandler) startSerfAgentHandler() error {
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
	defaultLogger.SetOutput(ioutil.Discard)
	handler.serfAgentHandlerObj = serfagenthandler.SerfAgentHandler{}
	handler.serfAgentHandlerObj.Name = handler.agentName
	handler.serfAgentHandlerObj.BindAddr = handler.addr
	handler.serfAgentHandlerObj.BindPort, _ = strconv.Atoi(handler.agentPort)
	handler.serfAgentHandlerObj.AgentLogger = defaultLogger.Default()
	handler.serfAgentHandlerObj.RpcAddr = handler.addr
	handler.serfAgentHandlerObj.RpcPort = handler.agentRPCPort
	//Start serf agent
	_, err := handler.serfAgentHandlerObj.Startup(handler.agentJoinAddrs)
	return err
}

func (handler *niovaKVServerHandler) startHTTPServer() error {
	//Start httpserver.
	handler.httpHandlerObj = httpserver.HttpServerHandler{}
	handler.httpHandlerObj.Addr = handler.addr
	handler.httpHandlerObj.Port = handler.httpPort
	handler.httpHandlerObj.NKVCliObj = handler.nkvClientObj
	if handler.needStats != "0" {
		handler.httpHandlerObj.NeedStats = true
	} 
	handler.httpHandlerObj.Limit, _ = strconv.Atoi(handler.limit)
	err := handler.httpHandlerObj.StartServer()
	return err
}

//Get gossip data
func (handler *niovaKVServerHandler) getGossipData() {
	tag := make(map[string]string)
	tag["Hport"] = handler.httpPort
	tag["Aport"] = handler.agentPort
	tag["Rport"] = handler.agentRPCPort
	handler.serfAgentHandlerObj.SetTags(tag)
	for {
		leader, err := handler.nkvClientObj.ClientObj.PmdbGetLeader()
		if err != nil {
			log.Error(err)
			//Wait for sometime to pmdb client to establish connection with raft cluster or raft cluster to appoint a leader
			time.Sleep(5 * time.Second)
			continue
		}
		tag["Leader UUID"] = leader.String()
		handler.serfAgentHandlerObj.SetTags(tag)
		log.Trace("(Niovakv Server)", tag)
		time.Sleep(300 * time.Millisecond)
	}
}

//Main func
func main() {

	niovaServerObj := niovaKVServerHandler{}
	//Get commandline paraameters.
	niovaServerObj.getCmdParams()

	flag.Usage = usage
	flag.Parse()
	if flag.NFlag() == 0 {
		usage()
		os.Exit(-1)
	}

	var err error
	//Create log file.
	err = PumiceDBCommon.InitLogger(niovaServerObj.logPath)
	switch niovaServerObj.logLevel {
	case "Info":
		log.SetLevel(log.InfoLevel)
	case "Trace":
		log.SetLevel(log.TraceLevel)
	}
	if err != nil {
		log.Error("(Niovakv Server) Logger error : ", err)
	}

	//get config data
	err = niovaServerObj.getConfigData()
	if err != nil {
		log.Panic("(Niovakv Server) Error while getting config data : ", err)
		os.Exit(1)
	}

	//Create a niovaKVServerHandler
	err = niovaServerObj.startNiovakvpmdbclient()
	if err != nil {
		log.Panic("(Niovakv Server) Error while starting pmdb client : ", err)
		os.Exit(1)
	}

	//Start serf agent handler
	err = niovaServerObj.startSerfAgentHandler()
	if err != nil {
		log.Panic("Error while starting serf agent : ", err)
		os.Exit(1)
	}

	//Start http server
	go func() {
		err = niovaServerObj.startHTTPServer()
		if err != nil {
			log.Panic("Error while starting http server : ", err)
		}
	}()

	//Start the gossip routine
        niovaServerObj.getGossipData()

}
