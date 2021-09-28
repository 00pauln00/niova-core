package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	PumiceDBCommon "niova/go-pumicedb-lib/common"
	"niovakv/clientapi"
	"niovakv/niovakvlib"
	"os"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
)

type niovakv_client struct {
	config_path, keyPrefix, valuePrefix, logPath, serial, noRequest, resultFile, operation string
	concurrency									       int
	respFillerLock                                                                         sync.Mutex
	operationMetaObjs                                                                      []opData //For filling json data
	operationsWait                                                                         sync.WaitGroup
	concurrency_channel								       chan int
	requestSentCount, failedRequestCount                                                   *int32
	nkvc                                                                                   *clientapi.ClientAPI
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
func (cli *niovakv_client) getCmdParams() {
	flag.StringVar(&cli.config_path, "c", "./config", "config file path")
	flag.StringVar(&cli.logPath, "l", ".", "log file path")
	flag.StringVar(&cli.keyPrefix, "k", "Key", "Key prefix")
	flag.StringVar(&cli.valuePrefix, "v", "Value", "Value prefix")
	flag.StringVar(&cli.serial, "s", "no", "Serialized request or not")
	flag.StringVar(&cli.noRequest, "n", "5", "No of request")
	flag.StringVar(&cli.resultFile, "r", "operation", "Path along with file name for the result file")
	flag.StringVar(&cli.operation, "o", "both", "Specify the opeation to perform in batch, leave it empty if both wites and reads are required")
	flag.IntVar(&cli.concurrency,"p",1,"No of concurrent execution")
	flag.Parse()
}

//Function to validate the read
func (cli *niovakv_client) validate_read(key string, value []byte) bool {
	key_prefix_len := len(cli.keyPrefix)
	key_identifier := []byte(key[key_prefix_len:])
	value_prefix_len := len(cli.valuePrefix)
	value_identifier := value[value_prefix_len:]
	return reflect.DeepEqual(key_identifier, value_identifier)
}

//Function to send the request
func (cli *niovakv_client) sendReq(req *niovakvlib.NiovaKV, write bool) {
	var (
		status   int
		resp     []byte
		validate bool
	)

	requestMeta := request{
		Opcode:    req.InputOps,
		Key:       req.InputKey,
		Value:     string(req.InputValue),
		Timestamp: time.Now(),
	}

	if write {
		status, resp = cli.nkvc.Put(req)
		validate = true
	} else {
		status, resp = cli.nkvc.Get(req)
		if status == 0 {
			validate = cli.validate_read(req.InputKey, resp)
		}
	}

	responseMeta := response{
		Timestamp:     time.Now(),
		Status:        status,
		Validate:      validate,
		ResponseValue: string(resp),
	}
	operationObj := opData{
		RequestData:  requestMeta,
		ResponseData: responseMeta,
		TimeDuration: responseMeta.Timestamp.Sub(requestMeta.Timestamp),
	}
	atomic.AddInt32(cli.requestSentCount, 1)
	if (responseMeta.Status == -1) || (!validate) {
		atomic.AddInt32(cli.failedRequestCount, 1)
	}
	//Lock the array to place the response
	cli.respFillerLock.Lock()
	cli.operationMetaObjs = append(cli.operationMetaObjs, operationObj)
	cli.respFillerLock.Unlock()

	cli.operationsWait.Done()
}

//Do reads and write
func (cli *niovakv_client) doWrite_Read(n int, write bool) []opData {
	var operation string
	cli.operationMetaObjs = nil
	if write {
		operation = "write"
	} else {
		operation = "read"
	}

	for i := 0; i < n; i++ {
		//Create request object
		requestObj := niovakvlib.NiovaKV{}
		requestObj.InputKey = cli.keyPrefix + strconv.Itoa(i)
		requestObj.InputOps = operation
		if write {
			requestObj.InputValue = []byte(cli.valuePrefix + strconv.Itoa(i))
		}

		//Send the request object
		cli.operationsWait.Add(1)
		cli.concurrency_channel <- 1
		go func(){
			cli.sendReq(&requestObj, write)
			_ = <-cli.concurrency_channel
		}()
		if cli.serial == "yes" {
			cli.operationsWait.Wait()
		}
	}

	//Wait till all request are completed
	cli.operationsWait.Wait()

	//Write summary
	cli.logSummary(operation, n)
	return cli.operationMetaObjs
}

//Log summary
func (cli *niovakv_client) logSummary(opcode string, n int) {
	sum := 0
	for _, ops := range cli.operationMetaObjs {
		sum += int(ops.TimeDuration.Milliseconds())
	}
	log.Info("Total no of operations and failed count in ", opcode, " : ", *cli.requestSentCount, *cli.failedRequestCount)
	log.Info("Avg ", opcode, " response time : ", sum/n, " milli sec")
}

//Write to Json
func (cli *niovakv_client) write2Json(toJson map[string][]opData) {
	file, _ := json.MarshalIndent(toJson, "", " ")
	_ = ioutil.WriteFile(cli.resultFile+".json", file, 0644)
}

//Print progress
func (cli *niovakv_client) printProgress(operation string, total_no_request int) {
	fmt.Println(" ")
	for atomic.LoadInt32(cli.requestSentCount) != int32(total_no_request) {
		fmt.Print("\033[G\033[K")
		fmt.Print("\033[A")
		fmt.Println(atomic.LoadInt32(cli.requestSentCount), " / ", total_no_request, operation, "request completed")
		time.Sleep(1 * time.Second)
	}
	fmt.Print("\033[G\033[K")
	fmt.Print("\033[A")
	fmt.Println(atomic.LoadInt32(cli.requestSentCount), " / ", total_no_request, operation, "request completed")
}

func main() {
	//Intialize client object
	clientObj := niovakv_client{}
	
	//Get commandline parameters.
	clientObj.getCmdParams()
	flag.Usage = usage
	if flag.NFlag() == 0 {
		usage()
		os.Exit(-1)
	}
	clientObj.concurrency_channel=make(chan int,clientObj.concurrency)
	//Create logger
	err := PumiceDBCommon.InitLogger(clientObj.logPath)
	if err != nil {
		log.Error("Error while initializing the logger  ", err)
	}

	log.Info("----START OF EXECUTION---")
	//Init niovakv client API
	clientObj.nkvc = &clientapi.ClientAPI{
		Timeout: 10,
	}
	stop := make(chan int)
	go func() {
		err := clientObj.nkvc.Start(stop, clientObj.config_path)
		if err != nil {
			log.Error(err)
			os.Exit(1)
		}
	}()
	clientObj.nkvc.Tillready()

	//Process the request
	n, _ := strconv.Atoi(clientObj.noRequest)
	//Decl and init required variables
	toJson := make(map[string][]opData)
	var fallthrough_flag bool
	var sent_count, failed_count int32
	clientObj.requestSentCount = &sent_count
	clientObj.failedRequestCount = &failed_count

	switch clientObj.operation {
	case "both":
		fallthrough_flag = true
		fallthrough

	case "write":
		go clientObj.printProgress("write", n)
		toJson["write"] = clientObj.doWrite_Read(n, true)

		//If to continue with read
		if !fallthrough_flag {
			clientObj.write2Json(toJson)
			break
		}

		sent_count = 0
		failed_count = 0
		fallthrough

	case "read":
		go clientObj.printProgress("read", n) 
		toJson["read"]=clientObj.doWrite_Read(n, false)
		clientObj.write2Json(toJson)

	case "membership":
		toJson := clientObj.nkvc.GetMembership()
		file, _ := json.MarshalIndent(toJson, "", " ")
		_ = ioutil.WriteFile(clientObj.resultFile+".json", file, 0644)

	case "leader":
		data := clientObj.nkvc.GetLeader()
		toJson := make(map[string]string, 1)
		toJson["Leader-UUID"] = data
		file, _ := json.MarshalIndent(toJson, "", " ")
		_ = ioutil.WriteFile(clientObj.resultFile+".json", file, 0644)
	}

	log.Info("----END OF EXECUTION---")

	//stop the member searcher
	stop <- 1

}
