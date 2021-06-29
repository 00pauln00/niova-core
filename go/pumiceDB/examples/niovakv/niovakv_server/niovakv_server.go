package main

import (
	"bufio"
	"flag"
	"fmt"
	defaultLog "log"
	"niovakv/niovakvpmdbclient"
	"niovakvserver/serfagenthandler"
	"os"
	"strconv"
	"strings"
	"time"

	"niovakv/httpserver"

	log "github.com/sirupsen/logrus"
)

var (
	raftUuid, clientUuid, logPath, serfConfigPath string
	configData                                    map[string]string
)

//Create logfile for client.
func initLogger() {

	var filename string = logPath + "/" + "niovaServer" + ".log"
	log.Info("logfile:", filename)

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
Name, Addr, Aport, Rport, Hport, Jaddr

Structure
key value

Name //For serf agent name, must be unique for each node
Addr //Addr for serf agent and http listening
Aport //Serf agent-agent communication
Rport //Serf agent-client communication
Hport //Http listener port
Jaddr //Other serf agent addrs seprated with ,
*/
func getData() error {
	reader, err := os.Open(serfConfigPath)
	if err != nil {
		return err
	}
	filescanner := bufio.NewScanner(reader)
	filescanner.Split(bufio.ScanLines)
	for filescanner.Scan() {
		input := strings.Split(filescanner.Text(), " ")
		configData[input[0]] = input[1]
	}
	return nil
}

//Function to get command line parameters while starting of the client.
func getCmdParams() {
	flag.StringVar(&raftUuid, "r", "NULL", "raft uuid")
	flag.StringVar(&clientUuid, "u", "NULL", "client uuid")
	flag.StringVar(&logPath, "l", "./", "log filepath")
	flag.StringVar(&serfConfigPath, "c", "./", "serf config path")

	flag.Parse()
}

func main() {

	//Get commandline paraameters.
	getCmdParams()

	//Create log file.
	initLogger()

	//Get client object.
	nkvclientObj := niovakvpmdbclient.GetNiovaKVClientObj(raftUuid, clientUuid, logPath)

	//Start pumicedb client.
	nkvclientObj.ClientObj.Start()

	//Wait for starting pumicedb client.
	time.Sleep(5 * time.Second)

	//Generate uuid.
	appUuid := uuid.NewV4().String()
	//rncui := appUuid + ":0:0:0:0"

	//Store rncui in nkvclientObj.
	nkvclientObj.Rncui = appUuid

	//get cmd line and config data
	configData = make(map[string]string, 10)
	err := getData()
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}

	//Start serf agent
	agentHandler := serfagenthandler.SerfAgentHandler{}
	agentHandler.Name = configData["Name"]
	agentHandler.BindAddr = configData["Addr"]
	agentHandler.BindPort, _ = strconv.Atoi(configData["Aport"])
	agentHandler.AgentLogger = defaultLog.Default()
	agentHandler.RpcAddr = configData["Addr"]
	agentHandler.RpcPort = configData["Rport"]
	var joinaddr []string
	if configData["Jaddr"] != "" {
		joinaddr = append(joinaddr, strings.Split(configData["Jaddr"], ",")...)
	}
	_, err = agentHandler.Startup(joinaddr)
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}

	//Start httpserver.
	handlerobj := httpserver.HttpServerHandler{}
	handlerobj.Addr = configData["Addr"]
	handlerobj.Port = configData["Hport"]
	handlerobj.NKVCliObj = nkvclientObj

	fmt.Println("Starting httpd server")
	errServer := handlerobj.StartServer()
	if errServer != nil {
		fmt.Println("Err occured while starting httpserver:", errServer)
	}
}
