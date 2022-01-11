package main

import (
	"bytes"
	"common/clientAPI"
	"common/requestResponseLib"
	"encoding/gob"
	"encoding/json"
	"flag"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	PumiceDBCommon "niova/go-pumicedb-lib/common"
	"os"
	"time"
)

type clientHandler struct {
	requestKey        string
	requestValue      string
	addr              string
	operation         string
	configPath        string
	logPath           string
	resultFile        string
	operationMetaObjs []opData //For filling json data
	clientAPIObj      clientAPI.ClientAPIHandler
}

type request struct {
	Opcode    string    `json:"Operation"`
	Key       string    `json:"Key"`
	Value     string    `json:"Value"`
	Timestamp time.Time `json:"Request_timestamp"`
}

type response struct {
	Status        int       `json:"Status"`
	ResponseValue string    `json:"Response"`
	validate      bool      `json:"validate"`
	Timestamp     time.Time `json:"Response_timestamp"`
}
type opData struct {
	RequestData  request       `json:"Request"`
	ResponseData response      `json:"Response"`
	TimeDuration time.Duration `json:"Req_resolved_time"`
}

func usage() {
	flag.PrintDefaults()
	os.Exit(0)
}

//Function to get command line parameters
func (handler *clientHandler) getCmdParams() {
	flag.StringVar(&handler.requestKey, "k", "Key", "Key")
	flag.StringVar(&handler.requestValue, "v", "Value", "Value")
	flag.StringVar(&handler.configPath, "c", "./gossipNodes", "gossip nodes file path")
	flag.StringVar(&handler.logPath, "l", "/tmp/temp.log", "Log path")
	flag.StringVar(&handler.operation, "o", "NULL", "Specify the opeation to perform")
	flag.StringVar(&handler.resultFile, "r", "operation", "Path along with file name for the result file")
	flag.Parse()
}

//Write to Json
func (cli *clientHandler) write2Json(toJson map[string][]opData) {
	file, _ := json.MarshalIndent(toJson, "", " ")
	_ = ioutil.WriteFile(cli.resultFile+".json", file, 0644)
}

func main() {
	//Intialize client object
	clientObj := clientHandler{}

	//Get commandline parameters.
	clientObj.getCmdParams()
	flag.Usage = usage
	if flag.NFlag() == 0 {
		usage()
		os.Exit(-1)
	}

	//Create logger
	err := PumiceDBCommon.InitLogger(clientObj.logPath)
	if err != nil {
		log.Error("Error while initializing the logger  ", err)
	}

	log.Info("----START OF EXECUTION---")

	//Init niovakv client API
	clientObj.clientAPIObj = clientAPI.ClientAPIHandler{
		Timeout: 10,
	}
	stop := make(chan int)
	go func() {
		err := clientObj.clientAPIObj.Start_ClientAPI(stop, clientObj.configPath)
		if err != nil {
			log.Error(err)
			os.Exit(1)
		}
	}()
	clientObj.clientAPIObj.Till_ready()

	//Send request
	var write bool
	requestObj := requestResponseLib.KVRequest{}
	responseObj := requestResponseLib.KVResponse{}

	//Decl and init required variables
	toJson := make(map[string][]opData)

	switch clientObj.operation {
	case "write":
		requestObj.Value = []byte(clientObj.requestValue)
		write = true
		fallthrough

	case "read":
		requestObj.Key = clientObj.requestKey
		requestObj.Operation = clientObj.operation
		var requestByte bytes.Buffer
		enc := gob.NewEncoder(&requestByte)
		enc.Encode(requestObj)
		//Send the write
		responseBytes := clientObj.clientAPIObj.Request(requestByte.Bytes(), "", write)
		dec := gob.NewDecoder(bytes.NewBuffer(responseBytes))
		err = dec.Decode(&responseObj)
		fmt.Println("Response:", string(responseObj.Value))

		//Creation of output json
		sendTime := time.Now()
		requestMeta := request{
			Opcode:    requestObj.Operation,
			Key:       requestObj.Key,
			Value:     string(responseObj.Value),
			Timestamp: sendTime,
		}

		responseMeta := response{
			Timestamp:     time.Now(),
			Status:        responseObj.Status,
			ResponseValue: string(responseObj.Value),
		}

		operationObj := opData{
			RequestData:  requestMeta,
			ResponseData: responseMeta,
			TimeDuration: responseMeta.Timestamp.Sub(requestMeta.Timestamp),
		}

		clientObj.operationMetaObjs = append(clientObj.operationMetaObjs, operationObj)
		if write{
			toJson["write"] = clientObj.operationMetaObjs
		} else{
			toJson["read"] = clientObj.operationMetaObjs
		}
		clientObj.write2Json(toJson)

	case "config":
		responseBytes,err:= clientObj.clientAPIObj.Get_PMDBServer_Config()
		log.Info("Response : ", string(responseBytes))
		if err != nil {
			log.Error("Unable to get the config data")
		}
		_ = ioutil.WriteFile(clientObj.resultFile+".json", responseBytes, 0644)

	case "membership":
                toJson := clientObj.clientAPIObj.Get_Membership()
                file, _ := json.MarshalIndent(toJson, "", " ")
                _ = ioutil.WriteFile(clientObj.resultFile+".json", file, 0644)

	}

	//clientObj.clientAPIObj.DumpIntoJson("./execution_summary.json")

}
