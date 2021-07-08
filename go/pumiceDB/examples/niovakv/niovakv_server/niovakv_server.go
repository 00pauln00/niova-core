package main

import (
	"bufio"
	"flag"
	"fmt"
	defaultLog "log"
	"os"
	"strconv"
	"strings"
	"time"

	uuid "github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"

	"niovakv/httpserver"
	"niovakv/niovakvpmdbclient"
	"niovakvserver/serfagenthandler"
)

type niovaKVServerHandler struct {
	//Other
	configPath string

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
	serfAgentHandlerObj serfagenthandler.SerfAgentHandler

	//Http
	httpPort       string
	httpHandlerObj httpserver.HttpServerHandler
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
	flag.Parse()
}

//Create logfile for client.
func (handler *niovaKVServerHandler) initLogger() {

	// Split logpath name.
	parts := strings.Split(handler.logPath, "/")
	fname := parts[len(parts)-1]
	dir := strings.TrimSuffix(handler.logPath, fname)

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.MkdirAll(dir, 0700) // Create directory
	}

	filename := dir + fname
	fmt.Println("logfile:", filename)

	//Create the log file if doesn't exist. And append to it if it already exists.i
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	Formatter := new(log.TextFormatter)

	//Set Timestamp format for logfile.
	Formatter.TimestampFormat = "02-01-2006 15:04:05"
	Formatter.FullTimestamp = true
	log.SetFormatter(Formatter)

	if err != nil {
		// Cannot open log file. Logging to stderr
		log.Error(err)
	} else {
		log.SetOutput(f)
	}
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
		log.Error("Agent name not provided or wrong agent name")
		os.Exit(1)
	}
	return nil
}

//start the Niovakvpmdbclient
func (handler *niovaKVServerHandler) startNiovakvpmdbclient() {
	//Get client object.
	fmt.Println(handler.raftUUID, handler.clientUUID, handler.logPath)
	handler.nkvClientObj = niovakvpmdbclient.GetNiovaKVClientObj(handler.raftUUID, handler.clientUUID, handler.logPath)
	//Start pumicedb client.
	handler.nkvClientObj.ClientObj.Start()
	//Store rncui in nkvclientObj.
	handler.nkvClientObj.AppUuid = uuid.NewV4().String()
}

//start the SerfAgentHandler
func (handler *niovaKVServerHandler) startSerfAgentHandler() {
	handler.serfAgentHandlerObj = serfagenthandler.SerfAgentHandler{}
	handler.serfAgentHandlerObj.Name = handler.agentName
	handler.serfAgentHandlerObj.BindAddr = handler.addr
	handler.serfAgentHandlerObj.BindPort, _ = strconv.Atoi(handler.agentPort)
	handler.serfAgentHandlerObj.AgentLogger = defaultLog.Default()
	handler.serfAgentHandlerObj.RpcAddr = handler.addr
	handler.serfAgentHandlerObj.RpcPort = handler.agentRPCPort
	fmt.Println("Serf handler object : ", handler.serfAgentHandlerObj)
	//Start serf agent
	_, err := handler.serfAgentHandlerObj.Startup(handler.agentJoinAddrs)
	if err != nil {
		log.Error("Error when starting agents : ", err)
	}
}

func (handler *niovaKVServerHandler) startHTTPServer() {
	//Start httpserver.
	handler.httpHandlerObj = httpserver.HttpServerHandler{}
	handler.httpHandlerObj.Addr = handler.addr
	handler.httpHandlerObj.Port = handler.httpPort
	handler.httpHandlerObj.NKVCliObj = handler.nkvClientObj
	log.Info("Starting httpd server")
	err := handler.httpHandlerObj.StartServer()
	if err != nil {
		log.Error(err)
	}
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
			os.Exit(1)
		}
		tag["Leader UUID"] = leader.String()
		fmt.Println(tag)
		handler.serfAgentHandlerObj.SetTags(tag)
		time.Sleep(5 * time.Second)
	}
}

//Main func
func main() {
	niovaServerObj := niovaKVServerHandler{}
	//Get commandline paraameters.
	niovaServerObj.getCmdParams()

	//Create log file.
	niovaServerObj.initLogger()

	//get cmd line and config data
	err := niovaServerObj.getConfigData()
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}

	//Create a niovaKVServerHandler
	niovaServerObj.startNiovakvpmdbclient()

	//Start serf agent handler
	niovaServerObj.startSerfAgentHandler()
	//Start the gossip routine
	time.Sleep(5 * time.Second)
	go niovaServerObj.getGossipData()

	//Start http server
	niovaServerObj.startHTTPServer()
}
