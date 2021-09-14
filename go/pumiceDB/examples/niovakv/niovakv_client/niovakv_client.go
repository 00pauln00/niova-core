package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	PumiceDBCommon "niova/go-pumicedb-lib/common"
	"niovakv/clientapi"
	"niovakv/niovakvlib"
	"niovakv/serfclienthandler"
	"os"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
)

var (
	ClientHandler                                                                          serfclienthandler.SerfClientHandler
	config_path, keyPrefix, valuePrefix, logPath, serial, noRequest, resultFile, operation string
	reqobjs_write, reqobjs_read                                                            []niovakvlib.NiovaKV
	respFillerLock                                                                         sync.Mutex
	operationMetaReadObjs, operationMetaWriteObjs                                          []opData //For filling json
	w                                                                                      sync.WaitGroup
	requestSentCount, failedRequestCount                                                   *int32
	nkvc                                                                                   *clientapi.NiovakvClient
)

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

//Create logfile for client.

//Function to get command line parameters while starting of the client.
func getCmdParams() {
	flag.StringVar(&config_path, "c", "./", "config file path")
	flag.StringVar(&logPath, "l", ".", "log file path")
	flag.StringVar(&keyPrefix, "k", "Key", "Key prefix")
	flag.StringVar(&valuePrefix, "v", "Value", "Value prefix")
	flag.StringVar(&serial, "s", "no", "Serialized request or not")
	flag.StringVar(&noRequest, "n", "5", "No of request")
	flag.StringVar(&resultFile, "r", "operation", "Path along with file name for the result file")
	flag.StringVar(&operation, "o", "both", "Specify the opeation to perform in batch, leave it empty if both wites and reads are required")
	flag.Parse()
}

func validate_read(key string, value []byte) bool {
	key_prefix_len := len(keyPrefix)
	key_identifier := []byte(key[key_prefix_len:])
	value_prefix_len := len(valuePrefix)
	value_identifier := value[value_prefix_len:]
	return reflect.DeepEqual(key_identifier, value_identifier)
}

func sendReq(req *niovakvlib.NiovaKV, write bool) {
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
		status, resp = nkvc.Put(req)
	} else {
		status, resp = nkvc.Get(req)
		if status == 0 {
			validate = validate_read(req.InputKey, resp)
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
	atomic.AddInt32(requestSentCount, 1)
	if (responseMeta.Status == -1) || (validate) {
		atomic.AddInt32(failedRequestCount, 1)
	}
	respFillerLock.Lock()
	if write {
		operationMetaWriteObjs = append(operationMetaWriteObjs, operationObj)
	} else {
		operationMetaReadObjs = append(operationMetaReadObjs, operationObj)

	}
	respFillerLock.Unlock()
	w.Done()
}

func doWrite_Read(reqs []niovakvlib.NiovaKV, n int, write bool) {
	for j := n - 1; j >= 0; j-- {
		w.Add(1)
		go sendReq(&reqs[j], write)
		if serial == "yes" {
			w.Wait()
		}
	}
	//Wait till all request are completed
	w.Wait()
}

func logSummary(operationMetaObjs *[]opData, opcode string, n int) {
	sum := 0
	for _, ops := range *operationMetaObjs {
		sum += int(ops.TimeDuration.Milliseconds())
	}
	log.Info("Total no of operations and failed count in ", opcode, " : ", *requestSentCount, *failedRequestCount)
	log.Info("Avg ", opcode, " response time : ", sum/n, " milli sec")
}

func write2Json(toJson map[string][]opData) {
	file, _ := json.MarshalIndent(toJson, "", " ")
	_ = ioutil.WriteFile(resultFile+".json", file, 0644)
}

func printProgress(operation string, total int) {
	fmt.Println(" ")
	for atomic.LoadInt32(requestSentCount) != int32(total) {
		fmt.Print("\033[G\033[K")
		fmt.Print("\033[A")
		fmt.Println(atomic.LoadInt32(requestSentCount), " / ", total, operation, "request completed")
		time.Sleep(1 * time.Second)
	}
	fmt.Print("\033[G\033[K")
	fmt.Print("\033[A")
	fmt.Println(atomic.LoadInt32(requestSentCount), " / ", total, operation, "request completed")
}

func main() {
	var total, failures int32
	total = 0
	failures = 0
	requestSentCount = &total
	failedRequestCount = &failures

	//Get commandline parameters.
	getCmdParams()
	flag.Usage = usage
	if flag.NFlag() == 0 {
		usage()
		os.Exit(-1)
	}

	//Create log file.
	err := PumiceDBCommon.InitLogger(logPath)
	if err != nil {
		log.Error("Error while initializing the logger  ", err)
	}

	log.Info("----START OF LOG---")

	//To init clientapi
	nkvc = &clientapi.NiovakvClient{
		Timeout: 10,
	}
	stop := make(chan int)
	go func() {
		err := nkvc.Start(stop, config_path)
		log.Error(err)
		os.Exit(1)
	}()

	time.Sleep(5 * time.Second)
	//Process request
	n, _ := strconv.Atoi(noRequest)
	toJson := make(map[string][]opData)
	var fallthrough_flag bool
	switch operation {
	case "both":
		fallthrough_flag = true
		fallthrough

	case "write":
		go printProgress("write", n)
		for i := 0; i < n; i++ {
			reqObj1 := niovakvlib.NiovaKV{}
			reqObj1.InputOps = "write"
			reqObj1.InputKey = keyPrefix + strconv.Itoa(i)
			reqObj1.InputValue = []byte(valuePrefix + strconv.Itoa(i))
			reqobjs_write = append(reqobjs_write, reqObj1)
		}
		doWrite_Read(reqobjs_write, n, true)
		logSummary(&operationMetaWriteObjs, "write", n)
		toJson["write"] = operationMetaWriteObjs

		//If to continue with read
		total = 0
		failures = 0
		if !fallthrough_flag {
			write2Json(toJson)
			break
		}
		fallthrough

	case "read":
		go printProgress("read", n)
		for i := 0; i < n; i++ {
			reqObj2 := niovakvlib.NiovaKV{}
			reqObj2.InputOps = "read"
			reqObj2.InputKey = keyPrefix + strconv.Itoa(i)
			reqobjs_read = append(reqobjs_read, reqObj2)
		}
		doWrite_Read(reqobjs_read, n, false)
		logSummary(&operationMetaReadObjs, "read", n)
		toJson["read"] = operationMetaReadObjs
		write2Json(toJson)

	case "membership":
		toJson := nkvc.GetMembership()
		file, _ := json.MarshalIndent(toJson, "", " ")
		_ = ioutil.WriteFile(resultFile+".json", file, 0644)

	case "leader":
		data := nkvc.GetLeader()
		toJson := make(map[string]string, 1)
		toJson["Leader-UUID"] = data
		file, _ := json.MarshalIndent(toJson, "", " ")
		_ = ioutil.WriteFile(resultFile+".json", file, 0644)
	}

	log.Info("----END OF LOG---")

}
