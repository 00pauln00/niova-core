package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"niovakv/niovakvclient"
	"niovakvserver/serfagenthandler"
	"os"
	"strconv"
	"strings"
	"time"

	"httpserver.com/httpserver"
)

var (
	raftUuid, clientUuid, jsonOutFpath, serfConfigPath string
	configData                                         map[string]string
)

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
		if len(input) == 2 {
			configData[input[0]] = input[1]
		}
	}
	return nil
}

//Function to get command line parameters while starting of the client.
func getCmdParams() {
	flag.StringVar(&raftUuid, "r", "NULL", "raft uuid")
	flag.StringVar(&clientUuid, "u", "NULL", "client uuid")
	flag.StringVar(&jsonOutFpath, "l", "./", "json_outfilepath")
	flag.StringVar(&serfConfigPath, "c", "./", "serf config path")

	flag.Parse()
	fmt.Println("Raft UUID: ", raftUuid)
	fmt.Println("Client UUID: ", clientUuid)
	fmt.Println("Json outfilepath:", jsonOutFpath)
	fmt.Println("Serf config path:", serfConfigPath)
}

func main() {

	//Get commandline paraameters.
	getCmdParams()

	//Get client object.
	nkvclientObj := niovakvclient.GetNiovaKVClientObj(raftUuid, clientUuid, jsonOutFpath)

	//Start pumicedb client.
	nkvclientObj.ClientObj.Start()

	//Wait for starting pumicedb client.
	time.Sleep(5 * time.Second)

	//Generate uuid.
	appUuid := uuid.NewV4().String()
	rncui := appUuid + ":0:0:0:0"

	//Store rncui in nkvclientObj.
	nkvclientObj.Rncui = rncui

	//get cmd line and config data
	configData = make(map[string]string, 10)
	err := getData()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	//Start serf agent
	agentHandler := serfagenthandler.SerfAgentHandler{}
	agentHandler.Name = configData["Name"]
	agentHandler.BindAddr = configData["Addr"]
	agentHandler.BindPort, _ = strconv.Atoi(configData["Aport"])
	agentHandler.AgentLogger = log.Default()
	agentHandler.RpcAddr = configData["Addr"]
	agentHandler.RpcPort = configData["Rport"]
	var joinaddr []string
	if configData["Jaddr"] != "" {
		joinaddr = append(joinaddr, strings.Split(configData["Jaddr"], ",")...)
	}
	_, err = agentHandler.Startup(joinaddr)
	if err != nil {
		fmt.Println(err)
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
