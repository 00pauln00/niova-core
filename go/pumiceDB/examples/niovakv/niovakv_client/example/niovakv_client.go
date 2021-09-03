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
	flag.Usage = usage
	if flag.NFlag() == 0 {
		usage()
		os.Exit(-1)
	}
}

func main() {
	var err error
	//Get commandline parameters.
	getCmdParams()

	//Create log file.
	err = PumiceDBCommon.InitLogger(logPath)
	if err != nil {
		log.Error("Error with logger : ", err)
	}

	var reqObj niovakvlib.NiovaKV
	var operationObj opData
	operationObj.RequestData = request{
		Opcode: operation,
		Key:    key,
	}
	reqObj.InputOps = operation
	reqObj.InputKey = key

	var send_stamp string
	var recv_stamp string
	var responseRecvd niovakvlib.NiovaKVResponse
	nkvc := clientapi.NiovakvClient{
		Timeout: 10,
	}
	stop := make(chan int)
	go func() {
		err := nkvc.Start(stop, config_path)
		log.Error(err)
		os.Exit(1)
	}()

	//Wait for membership table to get updated
	send_stamp = time.Now().String()
	switch operation {
	case "getLeader":
		responseRecvd.RespValue = []byte(nkvc.GetLeader())
	case "membership":
		toJson := nkvc.GetMembership()
		file, _ := json.MarshalIndent(toJson, "", " ")
		_ = ioutil.WriteFile(resultFile+".json", file, 0644)
		os.Exit(1)

	case "write":
		operationObj.RequestData.Value = value
		reqObj.InputValue = []byte(value)
		responseRecvd.RespStatus = nkvc.Put(&reqObj)

	case "read":
		responseRecvd.RespValue = nkvc.Get(&reqObj)
	}

	recv_stamp = time.Now().String()
	operationObj.RequestData.Timestamp = send_stamp
	operationObj.ResponseData = response{
		Timestamp:     recv_stamp,
		Status:        responseRecvd.RespStatus,
		ResponseValue: string(responseRecvd.RespValue),
	}

	//Stop the service and write to file
	stop <- 1
	toJson := make(map[string]opData)
	toJson[operation] = operationObj
	file, _ := json.MarshalIndent(toJson, "", " ")
	_ = ioutil.WriteFile(resultFile+".json", file, 0644)

}
