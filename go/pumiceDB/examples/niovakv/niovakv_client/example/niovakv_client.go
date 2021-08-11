package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"os"
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
	flag.PrintDefaults()
	os.Exit(0)
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
	nkvc := clientapi.NiovakvClient{}
	stop := make(chan int)
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

	//Result writing
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

}
