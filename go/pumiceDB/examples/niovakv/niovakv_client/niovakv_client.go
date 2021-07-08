package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"os"
	"time"

	log "github.com/sirupsen/logrus"

	"niovakv/httpclient"
	"niovakv/niovakvlib"
	"niovakv/serfclienthandler"
)

var (
	ClientHandler                                            serfclienthandler.SerfClientHandler
	config_path, operation, key, value, logPath, logFilename string
	retries                                                  int
)

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

//Create logfile for client.
func initLogger() {

	var filename string = logPath + "/" + logFilename + ".log"

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
	flag.StringVar(&config_path, "c", "./", "config file path")
	flag.StringVar(&logPath, "l", "./", "log filepath")
	flag.StringVar(&logFilename, "f", "niovakvClient", "log filename")
	flag.StringVar(&operation, "o", "NULL", "write/read operation")
	flag.StringVar(&key, "k", "NULL", "Key")
	flag.StringVar(&value, "v", "NULL", "Value")
	flag.Parse()
}

//Get any client addr
func getServerAddr(refresh bool) (string, string) {
	if refresh {
		ClientHandler.GetData(false)
	}
	//Get random addr
	if len(ClientHandler.Agents) <= 0 {
		log.Error("All servers are dead")
		os.Exit(1)
	}
	//randomIndex := rand.Intn(len(ClientHandler.Agents))
	randomNode := ClientHandler.Agents[retries%len(ClientHandler.Agents)]
	retries += 1
	return ClientHandler.AgentData[randomNode].Addr, ClientHandler.AgentData[randomNode].Tags["Hport"]
}

func main() {

	//Get commandline parameters.
	getCmdParams()

	//Create log file.
	initLogger()

	//For serf client init
	ClientHandler = serfclienthandler.SerfClientHandler{}
	ClientHandler.Retries = 5
	ClientHandler.AgentData = make(map[string]*serfclienthandler.Data)
	ClientHandler.Initdata(config_path)
	var reqObj niovakvlib.NiovaKV
	var operationObj opData
	var doOperation func(*niovakvlib.NiovaKV, string, string) (*niovakvlib.NiovaKVResponse, error)
	operationObj.RequestData = request{
		Opcode: operation,
		Key:    key,
	}
	reqObj.InputOps = operation
	reqObj.InputKey = key
	if operation == "write" {
		operationObj.RequestData.Value = value
		reqObj.InputValue = []byte(value)
		doOperation = httpclient.WriteRequest
	} else {
		doOperation = httpclient.ReadRequest
	}

	//Retry upto 5 times if request failed
	addr, port := getServerAddr(true)
	operationObj.RequestData.Sent_to = addr + ":" + port
	var send_stamp string
	var recv_stamp string
	var responseRecvd *niovakvlib.NiovaKVResponse
	var err error
	for j := 0; j < 5; j++ {
		send_stamp = time.Now().String()
		responseRecvd, err = doOperation(&reqObj, addr, port)
		recv_stamp = time.Now().String()
		if err == nil {
			break
		}
		addr, port = getServerAddr(false)
		log.Error(err)
	}
	operationObj.RequestData.Timestamp = send_stamp
	operationObj.ResponseData = response{
		Timestamp:     recv_stamp,
		Status:        responseRecvd.RespStatus,
		ResponseValue: string(responseRecvd.RespValue),
	}
	file, err := json.MarshalIndent(operationObj, "", " ")
	_ = ioutil.WriteFile("operation.json", file, 0644)
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
