package main

import (
	"bytes"
	"ctlplane/clientapi"
	"ctlplane/niovakvlib"
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

type ncp_client struct {
	reqKey            string
	reqValue          string
	addr              string
	operation         string
	configPath        string
	logPath           string
	resultFile        string
	operationMetaObjs []opData //For filling json data
	ncpc              clientapi.ClientAPI
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
func (cli *ncp_client) getCmdParams() {
	flag.StringVar(&cli.reqKey, "k", "Key", "Key prefix")
	flag.StringVar(&cli.reqValue, "v", "Value", "Value prefix")
	flag.StringVar(&cli.configPath, "c", "./gossipNodes", "Raft peer config")
	flag.StringVar(&cli.logPath, "l", "/tmp/temp.log", "Log path")
	flag.StringVar(&cli.operation, "o", "NULL", "Specify the opeation to perform")
	flag.StringVar(&cli.resultFile, "r", "operation", "Path along with file name for the result file")
	flag.Parse()
}

//Write to Json
func (cli *ncp_client) write2Json(toJson map[string][]opData) {
	file, _ := json.MarshalIndent(toJson, "", " ")
	_ = ioutil.WriteFile(cli.resultFile+".json", file, 0644)
}

func main() {
	//Intialize client object
	clientObj := ncp_client{}

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
	clientObj.ncpc = clientapi.ClientAPI{
		Timeout: 10,
	}
	stop := make(chan int)
	go func() {
		err := clientObj.ncpc.Start(stop, clientObj.configPath)
		if err != nil {
			log.Error(err)
			os.Exit(1)
		}
	}()
	clientObj.ncpc.Till_ready()
	//time.Sleep(5 * time.Second)
	//Send request
	var write bool
	requestObj := niovakvlib.NiovaKV{}
	responseObj := niovakvlib.NiovaKVResponse{}

	//Decl and init required variables
	toJson := make(map[string][]opData)

	switch clientObj.operation {
	case "write":
		requestObj.InputOps = clientObj.operation
		requestObj.InputKey = clientObj.reqKey
		requestObj.InputValue = []byte(clientObj.reqValue)
		write = true

		sendTime := time.Now()
		requestMeta := request{
			Opcode:    requestObj.InputOps,
			Key:       requestObj.InputKey,
			Value:     string(requestObj.InputValue),
			Timestamp: sendTime,
		}

		responseMeta := response{
			Timestamp:     time.Now(),
			Status:        responseObj.RespStatus,
			ResponseValue: string(responseObj.RespValue),
		}
		operationObj := opData{
			RequestData:  requestMeta,
			ResponseData: responseMeta,
			TimeDuration: responseMeta.Timestamp.Sub(requestMeta.Timestamp),
		}

		//Lock the array to place the response
		clientObj.operationMetaObjs = append(clientObj.operationMetaObjs, operationObj)

		toJson["write"] = clientObj.operationMetaObjs
		clientObj.write2Json(toJson)
		fallthrough

	case "read":
		requestObj.InputKey = clientObj.reqKey
		requestObj.InputOps = clientObj.operation
		var requestByte bytes.Buffer
		enc := gob.NewEncoder(&requestByte)
		enc.Encode(requestObj)
		responseByteArray := clientObj.ncpc.Request(requestByte.Bytes(), "", write)
		dec := gob.NewDecoder(bytes.NewBuffer(responseByteArray))
		err = dec.Decode(&responseObj)
		fmt.Println("Response:", string(responseObj.RespValue))

		sendTime := time.Now()

		requestMeta := request{
			Opcode:    requestObj.InputOps,
			Key:       requestObj.InputKey,
			Value:     string(responseObj.RespValue),
			Timestamp: sendTime,
		}

		responseMeta := response{
			Timestamp:     time.Now(),
			Status:        responseObj.RespStatus,
			ResponseValue: string(responseObj.RespValue),
		}

		operationObj := opData{
			RequestData:  requestMeta,
			ResponseData: responseMeta,
			TimeDuration: responseMeta.Timestamp.Sub(requestMeta.Timestamp),
		}

		clientObj.operationMetaObjs = append(clientObj.operationMetaObjs, operationObj)

		toJson["read"] = clientObj.operationMetaObjs
		clientObj.write2Json(toJson)

	case "config":
		responseByteArray := clientObj.ncpc.Request([]byte(clientObj.reqKey), "/config", false)
		fmt.Println("Response : ", string(responseByteArray))

		marshal_data, err := json.MarshalIndent(string(responseByteArray), "", " ")
		if err != nil {
			log.Error("Unable to parse the data")
		}
		_ = ioutil.WriteFile(clientObj.resultFile+".json", marshal_data, 0644)

	}

	clientObj.ncpc.DumpIntoJson("./execution_summary.json")

}
