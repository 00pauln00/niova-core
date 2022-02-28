package main

import (
	"bytes"
	"common/requestResponseLib"
	"common/clientAPI"
	"encoding/gob"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	PumiceDBCommon "niova/go-pumicedb-lib/common"
	"os"
	"strings"
	"time"

	uuid "github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
)

func usage() {
	flag.PrintDefaults()
	os.Exit(0)
}

//Function to get command line parameters
func (handler *clientHandler) getCmdParams() {
	flag.StringVar(&handler.requestKey, "k", "Key", "Key")
	flag.StringVar(&handler.addr, "a", "127.0.0.1", "Addr value")
	flag.StringVar(&handler.port, "p", "1999", "Port value")
	flag.StringVar(&handler.requestValue, "v", "NULL", "Value")
	flag.StringVar(&handler.configPath, "c", "./gossipNodes", "gossip nodes file path")
	flag.StringVar(&handler.logPath, "l", "/tmp/temp.log", "Log path")
	flag.StringVar(&handler.operation, "o", "NULL", "Specify the opeation to perform")
	flag.StringVar(&handler.resultFile, "r", "operation", "Path along with file name for the result file")
	flag.StringVar(&handler.rncui, "u", uuid.NewV4().String()+":0:0:0:1", "RNCUI for request")
	flag.Parse()
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
	clientObj.clientAPIObj = serviceDiscovery.ServiceDiscoveryHandler{
		Timeout: 10,
	}
	stop := make(chan int)
	go func() {
		err := clientObj.clientAPIObj.StartClientAPI(stop, clientObj.configPath)
		if err != nil {
			log.Error(err)
			os.Exit(1)
		}
	}()
	clientObj.clientAPIObj.TillReady()

	//Send request
	var write bool
	requestObj := requestResponseLib.KVRequest{}
	responseObj := requestResponseLib.KVResponse{}

	//Decl and init required variables
	toJson := make(map[string][]opData)
	value := make(map[string]string)
	value["IP_ADDR"] = clientObj.addr
	value["Port"] = clientObj.port
	valueByte, _ := json.Marshal(value)
	switch clientObj.operation {
	case "write":
		if clientObj.requestValue != "NULL" {
			requestObj.Value = []byte(clientObj.requestValue)
		} else {
			requestObj.Value = valueByte
		}
		requestObj.Rncui = clientObj.rncui
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
		if write {
			toJson["write"] = clientObj.operationMetaObjs
		} else {
			toJson["read"] = clientObj.operationMetaObjs
		}
		clientObj.write2Json(toJson)

	case "config":
		responseBytes, err := clientObj.clientAPIObj.GetPMDBServerConfig()
		log.Info("Response : ", string(responseBytes))
		if err != nil {
			log.Error("Unable to get the config data")
		}
		_ = ioutil.WriteFile(clientObj.resultFile+".json", responseBytes, 0644)

	case "membership":
		toJson := clientObj.clientAPIObj.GetMembership()
		file, _ := json.MarshalIndent(toJson, "", " ")
		_ = ioutil.WriteFile(clientObj.resultFile+".json", file, 0644)

	case "general":
		fmt.Printf("\033[2J")
		fmt.Printf("\033[2;0H")
		fmt.Print("UUID")
		fmt.Printf("\033[2;38H")
		fmt.Print("Type")
		fmt.Printf("\033[2;50H")
		fmt.Println("Status")
		offset := 3
		for {
			lineCounter := 0
			data := clientObj.clientAPIObj.GetMembership()
			for _, node := range data {
				currentLine := offset + lineCounter
				fmt.Print(node.Name)
				fmt.Printf("\033[%d;38H", currentLine)
				fmt.Print(node.Tags["Type"])
				fmt.Printf("\033[%d;50H", currentLine)
				fmt.Println(node.Status)
				lineCounter += 1
			}
			time.Sleep(2 * time.Second)
			fmt.Printf("\033[3;0H")
			for i := 0; i < lineCounter; i++ {
				fmt.Println("                                                       ")
			}
			fmt.Printf("\033[3;0H")
		}
	case "nisd":
		fmt.Printf("\033[2J")
		fmt.Printf("\033[2;0H")
		fmt.Println("NISD_UUID")
		fmt.Printf("\033[2;38H")
		fmt.Print("Status")
		fmt.Printf("\033[2;45H")
		fmt.Println("Parent_UUID(Lookout)")
		offset := 3
		for {
			lineCounter := 0
			data := clientObj.clientAPIObj.GetMembership()
			for _, node := range data {
				if (node.Tags["Type"] == "LOOKOUT") && (node.Status == "alive") {
					for uuid, value := range node.Tags {
						if uuid != "Type" {
							currentLine := offset + lineCounter
							fmt.Print(uuid)
							fmt.Printf("\033[%d;38H", currentLine)
							fmt.Print(strings.Split(value, "_")[0])
							fmt.Printf("\033[%d;45H", currentLine)
							fmt.Println(node.Name)
							lineCounter += 1
						}
					}
				}
			}
			time.Sleep(2 * time.Second)
			fmt.Printf("\033[3;0H")
			for i := 0; i < lineCounter; i++ {
				fmt.Println("                                                       ")
			}
			fmt.Printf("\033[3;0H")
		}

	case "NISDGossip":
		nisdDataMap := clientObj.putNISDInfo()
		file, _ := json.MarshalIndent(nisdDataMap, "", " ")
		_ = ioutil.WriteFile(clientObj.resultFile+".json", file, 0644)
	}

	//clientObj.clientAPIObj.DumpIntoJson("./execution_summary.json")

}
