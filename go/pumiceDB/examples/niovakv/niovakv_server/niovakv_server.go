package main

import (
	"bufio"
	"errors"
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
	limit          string
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
	flag.StringVar(&handler.limit, "e", "10", "No of concurrent request")
	flag.Parse()
}

//Create logfile for client.
func (handler *niovaKVServerHandler) initLogger() error {

	// Split logpath name.
	parts := strings.Split(handler.logPath, "/")
	fname := parts[len(parts)-1]
	dir := strings.TrimSuffix(handler.logPath, fname)

	// Create directory if not exist.
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.MkdirAll(dir, 0700)
	}

	filename := dir + fname
	fmt.Println("logfile:", filename)

	//Create the log file if doesn't exist. And append to it if it already exists.
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
	return err
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
	handler.serfAgentHandlerObj = serfagenthandler.SerfAgentHandler{}
	handler.serfAgentHandlerObj.Name = handler.agentName
	handler.serfAgentHandlerObj.BindAddr = handler.addr
	handler.serfAgentHandlerObj.BindPort, _ = strconv.Atoi(handler.agentPort)
	handler.serfAgentHandlerObj.AgentLogger = defaultLog.Default()
	handler.serfAgentHandlerObj.RpcAddr = handler.addr
	handler.serfAgentHandlerObj.RpcPort = handler.agentRPCPort
	//Start serf agent
	_, err := handler.serfAgentHandlerObj.Startup(handler.agentJoinAddrs)
	if err != nil {
		log.Error("Error when starting agents : ", err)
	}
	return err
}

func (handler *niovaKVServerHandler) startHTTPServer() error {
	//Start httpserver.
	handler.httpHandlerObj = httpserver.HttpServerHandler{}
	handler.httpHandlerObj.Addr = handler.addr
	handler.httpHandlerObj.Port = handler.httpPort
	handler.httpHandlerObj.NKVCliObj = handler.nkvClientObj
	handler.httpHandlerObj.Limit, _ = strconv.Atoi(handler.limit)
	log.Info("Starting httpd server")
	err := handler.httpHandlerObj.StartServer()
	if err != nil {
		log.Error(err)
	}
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
			os.Exit(1)
		}
		tag["Leader UUID"] = leader.String()
		log.Info(tag)
		handler.serfAgentHandlerObj.SetTags(tag)
		time.Sleep(5 * time.Second)
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
	err = niovaServerObj.initLogger()
	if err != nil {
		log.Error("Logger error : ", err)
		os.Exit(1)
	}

	//get config data
	err = niovaServerObj.getConfigData()
	if err != nil {
		log.Error("Error while getting config data : ", err)
		os.Exit(1)
	}

	//Create a niovaKVServerHandler
	err = niovaServerObj.startNiovakvpmdbclient()
	if err != nil {
		log.Error("Error while starting pmdb client : ", err)
		os.Exit(1)
	}

	//Start serf agent handler
	err = niovaServerObj.startSerfAgentHandler()
	if err != nil {
		log.Error("Error while starting serf agent : ", err)
		os.Exit(1)
	}
	//Start the gossip routine
	time.Sleep(5 * time.Second)
	go niovaServerObj.getGossipData()

	//Start http server
	err = niovaServerObj.startHTTPServer()
	if err != nil {
		log.Error("Error while starting http server : ", err)
	}
}
