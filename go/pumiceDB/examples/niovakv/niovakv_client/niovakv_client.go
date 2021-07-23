package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"errors"
	"math/rand"
	"os"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"niovakv/httpclient"
	"niovakv/niovakvlib"
	"niovakv/serfclienthandler"
)

var (
	ClientHandler                                            serfclienthandler.SerfClientHandler
	config_path, operation, key, value, logPath, resultFile  string
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

func usage() {
	fmt.Printf("usage : %s -c <serf configs> -l <log directory> -o <write/read> -k <key> -v <value>\n", os.Args[0])
	os.Exit(0)
}

//Create logfile for client.
func initLogger() error{

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
	return err
}

//Function to get command line parameters while starting of the client.
func getCmdParams() {
	flag.StringVar(&config_path, "c", "./", "config file path")
	flag.StringVar(&logPath, "l", "./", "log filepath")
	flag.StringVar(&operation, "o", "NULL", "write/read operation")
	flag.StringVar(&key, "k", "NULL", "Key")
	flag.StringVar(&value, "v", "NULL", "Value")
	flag.StringVar(&resultFile,"r","operation","Result file")
	flag.Parse()
}

//Get any client addr
func getServerAddr(refresh bool) (string, string, error) {
	var err error
	//If update data
	if refresh {
		ClientHandler.GetData(false)
	}
	//Get random addr
	if len(ClientHandler.Agents) <= 0 {
		return "","",errors.New("All Nodes Down")
	}
	randomIndex := rand.Intn(len(ClientHandler.Agents))
	randomNode := ClientHandler.Agents[randomIndex]
	return ClientHandler.AgentData[randomNode].Addr, ClientHandler.AgentData[randomNode].Tags["Hport"], err
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
	err=initLogger()
	if err!=nil{
		log.Error("Error with logger : ",err)
	}
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
		doOperation = httpclient.PutRequest
	} else {
		doOperation = httpclient.GetRequest
	}

	//Retry upto 5 times if request failed
	addr, port, errAddr := getServerAddr(true)
	if errAddr != nil {
		log.Error(errAddr)
		os.Exit(1)
	}

	operationObj.RequestData.Sent_to = addr + ":" + port
	var send_stamp string
	var recv_stamp string
	var responseRecvd *niovakvlib.NiovaKVResponse
	for j := 0; j < 5; j++ {
		send_stamp = time.Now().String()
		responseRecvd, err = doOperation(&reqObj, addr, port)
		recv_stamp = time.Now().String()
		if err == nil {
			break
		}
		log.Error(err)
		addr, port, err = getServerAddr(false)
		if err != nil {
			log.Error(err)
			os.Exit(1)
		}
	}
	operationObj.RequestData.Timestamp = send_stamp
	operationObj.ResponseData = response{
		Timestamp:     recv_stamp,
		Status:        responseRecvd.RespStatus,
		ResponseValue: string(responseRecvd.RespValue),
	}
	file, _ := json.MarshalIndent(operationObj, "", " ")
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
