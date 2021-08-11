package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"niovakv/clientapi"
	PumiceDBCommon "niovakv/common"
	"niovakv/niovakvlib"
	"niovakv/serfclienthandler"

	log "github.com/sirupsen/logrus"
)

var (
	ClientHandler                                           serfclienthandler.SerfClientHandler
	config_path, operation, key, value, logPath, resultFile string
)

type AllNodesDown struct{}

func (m *AllNodesDown) Error() string {
	return "All Nodes Down"
}

type request struct {
	Opcode    string `json:"Operation"`
	Key       string `json:"Key"`
	Value     string `json:"Value"`
	Sent_to   string `json:Sent_to`
	Timestamp string `json:"Request_timestamp"`
}
type response struct {
	Status        int    `json:"Status"`
	ResponseValue string `json:"Response"`
	Timestamp     string `json:"Response_timestamp"`
}
type opData struct {
	RequestData  request  `json:"Request"`
	ResponseData response `json:"Response"`
}

func usage() {
	fmt.Printf("usage : %s -c <serf configs> -l <log directory> -o <write/read> -k <key> -v <value>\n", os.Args[0])
	os.Exit(0)
}

//Create logfile for client.
func initLogger() {

	//Split log directory path.
	parts := strings.Split(logPath, "/")
	fname := parts[len(parts)-1]
	dir := strings.TrimSuffix(logPath, fname)

	//Create directory if not exist.
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.MkdirAll(dir, 0700) // Create directory
	}

	filename := dir + fname
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

//Function to get command line parameters while starting of the client.
func getCmdParams() {
	flag.StringVar(&config_path, "c", "../", "config file path")
	flag.StringVar(&logPath, "l", "./", "log filepath")
	flag.StringVar(&operation, "o", "NULL", "write/read operation")
	flag.StringVar(&key, "k", "NULL", "Key")
	flag.StringVar(&value, "v", "NULL", "Value")
	flag.StringVar(&resultFile, "r", "operation", "Request file")
	flag.Parse()
}

func main() {
	var err error
	//Get commandline parameters.
	getCmdParams()

	flag.Usage = usage
	flag.Parse()

	if flag.NFlag() == 0 {
		usage()
		os.Exit(-1)
	}

	//Create log file.
	err = PumiceDBCommon.InitLogger(logPath)
	if err != nil {
		log.Error("Error with logger : ", err)
	}
	//For serf client init
	ClientHandler = serfclienthandler.SerfClientHandler{}
	ClientHandler.Retries = 5
	err = ClientHandler.Initdata(config_path)
	if err != nil {
		log.Error("Error while trying to read config file : ", err)
	}
	var reqObj niovakvlib.NiovaKV
	var operationObj opData
	// do operatin not needed
	operationObj.RequestData = request{
		Opcode: operation,
		Key:    key,
	}

	//collect basic information for read/write
	reqObj.InputOps = operation
	reqObj.InputKey = key

	// operationObj.RequestData.Sent_to = addr + ":" + port
	var send_stamp string
	var recv_stamp string
	var responseRecvd niovakvlib.NiovaKVResponse
	stop := make(chan int)
	nkvc := clientapi.NiovakvClient{}
	nkvc.Start(stop)
	switch operation {
	case "getLeader":
		req := request{
			Opcode: operation,
		}
		ClientHandler.GetData(true)
		node := ClientHandler.Agents[0]
		res := response{
			ResponseValue: node.Tags["Leader UUID"],
		}
		operationObj.RequestData = req
		operationObj.ResponseData = res

	case "membership":
		ClientHandler.GetData(true)
		toJson := ClientHandler.GetMemberListMap()
		file, _ := json.MarshalIndent(toJson, "", " ")
		_ = ioutil.WriteFile(resultFile+".json", file, 0644)
		os.Exit(1)

	case "write":
		operationObj.RequestData.Value = value
		reqObj.InputValue = []byte(value)
		send_stamp = time.Now().String()
		responseRecvd.RespStatus = nkvc.Put(&reqObj)
		recv_stamp = time.Now().String()
	case "read":
		nkvc.Start(stop)
		send_stamp = time.Now().String()
		responseRecvd.RespValue = nkvc.Get(&reqObj)
		recv_stamp = time.Now().String()
	}
	operationObj.RequestData.Timestamp = send_stamp
	operationObj.ResponseData = response{
		Timestamp:     recv_stamp,
		Status:        responseRecvd.RespStatus,
		ResponseValue: string(responseRecvd.RespValue),
	}

	stop <- 1
	toJson := make(map[string]opData)
	toJson[operation] = operationObj
	file, _ := json.MarshalIndent(toJson, "", " ")
	_ = ioutil.WriteFile(resultFile+".json", file, 0644)
	/*
		Following in the json file
		Request
			Operation type
			Key
			Value
			Sent_Timestamp
		Response
			Status
			Response
			Recvd_Timestamp
	*/
}
