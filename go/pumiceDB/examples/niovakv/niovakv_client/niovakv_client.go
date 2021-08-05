package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	PumiceDBCommon "niova/go-pumicedb-lib/common"
	"niovakv/httpclient"
	"niovakv/niovakvlib"
	"niovakv/serfclienthandler"
	"os"
	"time"

	log "github.com/sirupsen/logrus"
)

var (
	ClientHandler                                           serfclienthandler.SerfClientHandler
	config_path, operation, key, value, logPath, resultFile string
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
	flag.PrintDefaults()
	os.Exit(0)
}

//Function to get command line parameters while starting of the client.
func getCmdParams() {
	flag.StringVar(&config_path, "c", "./", "config file path")
	flag.StringVar(&logPath, "l", "./", "log filepath")
	flag.StringVar(&operation, "o", "NULL", "write/read operation")
	flag.StringVar(&key, "k", "NULL", "Key")
	flag.StringVar(&value, "v", "NULL", "Value")
	flag.StringVar(&resultFile, "r", "operation", "Result file")
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
		return "", "", errors.New("all nodes down")
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
	err = PumiceDBCommon.InitLogger(logPath)
	if err != nil {
		log.Error("Error with logger : ", err)
	}
	//For serf client init
	ClientHandler = serfclienthandler.SerfClientHandler{}
	ClientHandler.Retries = 5
	ClientHandler.AgentData = make(map[string]*serfclienthandler.Data)
	err = ClientHandler.Initdata(config_path)
	if err != nil {
		log.Error("Error while trying to read config file : ", err)
	}
	var reqObj niovakvlib.NiovaKV
	var operationObj opData
	var doOperation func(*niovakvlib.NiovaKV, string, string) (*niovakvlib.NiovaKVResponse, error)
	operationObj.RequestData = request{
		Opcode: operation,
		Key:    key,
	}
	reqObj.InputOps = operation
	reqObj.InputKey = key

	switch operation {
	case "getLeader":
		req := request{
			Opcode: operation,
		}
		ClientHandler.GetData(false)
		node := ClientHandler.Agents[0]
		res := response{
			ResponseValue: ClientHandler.AgentData[node].Tags["Leader UUID"],
		}
		operationObj.RequestData = req
		operationObj.ResponseData = res

	case "write":
		operationObj.RequestData.Value = value
		reqObj.InputValue = []byte(value)
		doOperation = httpclient.PutRequest
	case "read":
		doOperation = httpclient.GetRequest
	default:
		log.Error("Enter valid operation")
		os.Exit(1)
	}

	//Only if read/write
	if doOperation != nil {
		addr, port, errAddr := getServerAddr(true)
		if errAddr != nil {
			log.Error(errAddr)
			os.Exit(1)
		}

		operationObj.RequestData.Sent_to = addr + ":" + port
		var send_stamp string
		var recv_stamp string
		var responseRecvd *niovakvlib.NiovaKVResponse

		//Retry upto 5 times if request failed
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
	}
	toJson := make(map[string]opData)
	toJson[operation] = operationObj
	file, _ := json.MarshalIndent(toJson, "", " ")
	_ = ioutil.WriteFile(resultFile+".json", file, 0644)

	switch operation {
	case "write":
		fmt.Printf("Request =>  Key : %s, Value : %s\n", operationObj.RequestData.Key, operationObj.RequestData.Value)
		fmt.Printf("Response =>  Status : %d\n", operationObj.ResponseData.Status)
	case "read":
		fmt.Printf("Request =>  Key : %s \n", operationObj.RequestData.Key)
		fmt.Printf("Response =>  Value : %s\n", operationObj.ResponseData.ResponseValue)
	}
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
