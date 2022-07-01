package main

import (
	"bytes"
	"common/clientAPI"
	"common/requestResponseLib"
	"encoding/gob"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	PumiceDBCommon "niova/go-pumicedb-lib/common"
	"os"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
)

type clientHandler struct {
	configPath, keyPrefix, valuePrefix, logPath, serial, noRequest, resultFile, operation string
	concurrency                                                                           int
	respFillerLock                                                                        sync.Mutex
	operationMetaObjs                                                                     []opData //For filling json data
	operationsWait                                                                        sync.WaitGroup
	concurrencyChannel                                                                    chan int
	requestSentCount, failedRequestCount                                                  int64
	clientAPIObj                                                                          *serviceDiscovery.ServiceDiscoveryHandler
}

type request struct {
	Opcode    string    `json:"Operation"`
	Key       string    `json:"Key"`
	Value     string    `json:"Value"`
	Hash      [16]byte  `json:"CheckSum"`
	Timestamp time.Time `json:"Request_timestamp"`
}

type response struct {
	Status        int       `json:"Status"`
	ResponseValue string    `json:"Response"`
	Validate      bool      `json:"Validate"`
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
	flag.StringVar(&handler.configPath, "c", "./config", "config file path")
	flag.StringVar(&handler.logPath, "l", ".", "log file path")
	flag.StringVar(&handler.keyPrefix, "k", "Key", "Key prefix")
	flag.StringVar(&handler.valuePrefix, "v", "Value", "Value prefix")
	flag.StringVar(&handler.serial, "s", "no", "Serialized request or not")
	flag.StringVar(&handler.noRequest, "n", "5", "No of request")
	flag.StringVar(&handler.resultFile, "r", "operation", "Path along with file name for the result file")
	flag.StringVar(&handler.operation, "o", "both", "Specify the opeation to perform in batch, leave it empty if both wites and reads are required")
	flag.IntVar(&handler.concurrency, "p", 10, "No of concurrent execution")
	flag.Parse()
}

//Function to validate the read
func (handler *clientHandler) validate_read(key string, value []byte) bool {
	keyPrefixLen := len(handler.keyPrefix)
	keyIdentifier := []byte(key[keyPrefixLen:])
	valuePrefixLen := len(handler.valuePrefix)
	valueIdentifier := value[valuePrefixLen:]
	return reflect.DeepEqual(keyIdentifier, valueIdentifier)
}

//Function to send the request
func (handler *clientHandler) sendReq(req *requestResponseLib.KVRequest, write bool) {
	//Record request start time
	sendTime := time.Now()

	var status int
	var responseValue string
	var requestByte bytes.Buffer
	var validate bool
	enc := gob.NewEncoder(&requestByte)
	enc.Encode(req)

	responseBytes, err := handler.clientAPIObj.Request(requestByte.Bytes(), "", write)

	if err == nil {
		var responseObj requestResponseLib.KVResponse
		dec := gob.NewDecoder(bytes.NewBuffer(responseBytes))
		dec.Decode(&responseObj)

		if !write {
			if responseObj.Status == 0 {
				validate = handler.validate_read(req.Key, responseObj.Value)
			}
		}

		status = responseObj.Status
		responseValue = string(responseObj.Value)
	} else {
		status = 1
		responseValue = err.Error()
	}

	requestMeta := request{
		Opcode:    req.Operation,
		Key:       req.Key,
		Value:     string(req.Value),
		Hash:      req.CheckSum,
		Timestamp: sendTime,
	}

	responseMeta := response{
		Timestamp:     time.Now(),
		Status:        status,
		Validate:      validate,
		ResponseValue: responseValue,
	}

	operationObj := opData{
		RequestData:  requestMeta,
		ResponseData: responseMeta,
		TimeDuration: responseMeta.Timestamp.Sub(requestMeta.Timestamp),
	}

	atomic.AddInt64(&handler.requestSentCount, 1)
	if (err != nil) || (responseMeta.Status == -1) || (!validate) {
		atomic.AddInt64(&handler.failedRequestCount, int64(1))
	}

	//Lock the array to place the response
	handler.respFillerLock.Lock()
	handler.operationMetaObjs = append(handler.operationMetaObjs, operationObj)
	handler.respFillerLock.Unlock()

	handler.operationsWait.Done()
}

//Do reads and write
func (handler *clientHandler) do_WriteNRead(n int, write bool) []opData {
	var operation string
	handler.operationMetaObjs = nil
	if write {
		operation = "write"
	} else {
		operation = "read"
	}

	for i := 0; i < n; i++ {
		//Create request object
		requestObj := requestResponseLib.KVRequest{}
		requestObj.Key = handler.keyPrefix + strconv.Itoa(i)
		requestObj.Operation = operation
		if write {
			requestObj.Value = []byte(handler.valuePrefix + strconv.Itoa(i))
		}

		//Send the request object
		handler.operationsWait.Add(1)
		handler.concurrencyChannel <- 1
		go func() {
			handler.sendReq(&requestObj, write)
			_ = <-handler.concurrencyChannel
		}()
	}

	//Wait till all request are completed
	handler.operationsWait.Wait()

	//Write summary
	handler.logSummary(operation, n)
	return handler.operationMetaObjs
}

//Log summary
func (handler *clientHandler) logSummary(opcode string, n int) {
	sum := 0
	for _, ops := range handler.operationMetaObjs {
		sum += int(ops.TimeDuration.Milliseconds())
	}
	log.Info("Total no of operations and failed count in ", opcode, " : ", handler.requestSentCount, handler.failedRequestCount)
	log.Info("Avg ", opcode, " response time : ", sum/n, " milli sec")
}

//Write to Json
func (handler *clientHandler) write_Json(toJson map[string][]opData) {
	file, _ := json.MarshalIndent(toJson, "", " ")
	_ = ioutil.WriteFile(handler.resultFile+".json", file, 0644)
}

//Print progress
func (handler *clientHandler) print_progress(operation string, totalRequest int) {
	fmt.Println(" ")
	for handler.requestSentCount != int64(totalRequest) {
		fmt.Print("\033[G\033[K")
		fmt.Print("\033[A")
		fmt.Println(handler.requestSentCount, " / ", totalRequest, operation, "request completed")
		time.Sleep(1 * time.Second)
	}
	fmt.Print("\033[G\033[K")
	fmt.Print("\033[A")
	fmt.Println(handler.requestSentCount, " / ", totalRequest, operation, "request completed")
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

	//If serial set the concurrency level to 1
	if clientObj.serial == "y" {
		clientObj.concurrency = 1
	}
	clientObj.concurrencyChannel = make(chan int, clientObj.concurrency)

	//Create logger
	err := PumiceDBCommon.InitLogger(clientObj.logPath)
	if err != nil {
		log.Error("Error while initializing the logger  ", err)
	}

	log.Info("----START OF EXECUTION---")
	//Init niovakv client API
	clientObj.clientAPIObj = &serviceDiscovery.ServiceDiscoveryHandler{
		HTTPRetry : 10,
		SerfRetry : 5,
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

	//Process the request
	n, _ := strconv.Atoi(clientObj.noRequest)
	//Decl and init required variables
	toJson := make(map[string][]opData)
	var fallthroughFlag bool

	switch clientObj.operation {
	case "both":
		fallthroughFlag = true
		fallthrough

	case "write":
		go clientObj.print_progress("write", n)
		toJson["write"] = clientObj.do_WriteNRead(n, true)

		//If to continue with read
		if !fallthroughFlag {
			clientObj.write_Json(toJson)
			break
		}

		clientObj.requestSentCount = 0
		clientObj.failedRequestCount = 0
		fallthrough

	case "read":
		go clientObj.print_progress("read", n)
		toJson["read"] = clientObj.do_WriteNRead(n, false)
		clientObj.write_Json(toJson)

	case "membership":
		toJson := clientObj.clientAPIObj.GetMembership()
		file, _ := json.MarshalIndent(toJson, "", " ")
		_ = ioutil.WriteFile(clientObj.resultFile+".json", file, 0644)

	case "leader":
		data := clientObj.clientAPIObj.GetLeader()
		toJson := make(map[string]string, 1)
		toJson["Leader-UUID"] = data
		file, _ := json.MarshalIndent(toJson, "", " ")
		_ = ioutil.WriteFile(clientObj.resultFile+".json", file, 0644)
	}

	log.Info("----END OF EXECUTION---")

	//stop the member searcher
	stop <- 1

}
