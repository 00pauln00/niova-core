package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	PumiceDBCommon "niova/go-pumicedb-lib/common"
	"niovakv/httpclient"
	"niovakv/niovakvlib"
	"niovakv/serfclienthandler"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
)

var (
	ClientHandler                                                   serfclienthandler.SerfClientHandler
	config_path, key, value, logPath, serial, noRequest, resultFile string
	reqobjs_write, reqobjs_read                                     []niovakvlib.NiovaKV
	respFillerLock                                                  sync.Mutex
	operationMetaReadObjs, operationMetaWriteObjs                   []opData //For filling json
	w                                                               sync.WaitGroup
	requestSentCount, timeoutCount                                  *int32
)

type request struct {
	Opcode    string    `json:"Operation"`
	Key       string    `json:"Key"`
	Value     string    `json:"Value"`
	Sent_to   string    `json:Sent_to`
	Timestamp time.Time `json:"Request_timestamp"`
}
type response struct {
	Status        int       `json:"Status"`
	ResponseValue string    `json:"Response"`
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
	flag.StringVar(&key, "k", "Key", "Key prefix")
	flag.StringVar(&value, "v", "Value", "Value prefix")
	flag.StringVar(&serial, "s", "no", "Serialized request or not")
	flag.StringVar(&noRequest, "n", "5", "No of request")
	flag.StringVar(&resultFile, "r", "operation", "Path along with file name for the result file")
	flag.Parse()
}

func sendReq(req *niovakvlib.NiovaKV, addr string, port string, write bool) {
        w.Add(1)
	requestMeta := request{
		Opcode:    req.InputOps,
		Key:       req.InputKey,
		Value:     string(req.InputValue),
		Sent_to:   addr + ":" + port,
		Timestamp: time.Now(),
	}
	var resp *niovakvlib.NiovaKVResponse
	if write {
		resp, _ = httpclient.PutRequest(req, addr, port)
	} else {
		resp, _ = httpclient.GetRequest(req, addr, port)
	}
	responseMeta := response{
		Timestamp:     time.Now(),
		Status:        resp.RespStatus,
		ResponseValue: string(resp.RespValue),
	}
	operationObj := opData{
		RequestData:  requestMeta,
		ResponseData: responseMeta,
		TimeDuration: responseMeta.Timestamp.Sub(requestMeta.Timestamp),
	}
	atomic.AddInt32(requestSentCount, 1)
	if responseMeta.Status == -1 {
		atomic.AddInt32(timeoutCount, 1)
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

//Get any client addr
func getServerAddr(refresh bool) (string, string) {
	if refresh {
		ClientHandler.GetData(false)
	}
	//Get random addr
	if len(ClientHandler.Agents) <= 0 {
		log.Error("No live agents")
		os.Exit(1)
	}

	randomIndex := rand.Intn(len(ClientHandler.Agents))
	node := ClientHandler.Agents[randomIndex]
	if node.Tags["Hport"] == "" {
		return getServerAddr(true)
	}
	return node.Addr.String(), node.Tags["Hport"]
}

func main() {
	var total, failures int32
	total = 0
	failures = 0
	requestSentCount = &total
	timeoutCount = &failures
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

	//For serf client init
	ClientHandler = serfclienthandler.SerfClientHandler{}
	ClientHandler.Initdata(config_path)
	ClientHandler.Retries = 5

	//Create Write and read request
	n, _ := strconv.Atoi(noRequest)
	for i := 0; i < n; i++ {
		reqObj1 := niovakvlib.NiovaKV{}
		reqObj1.InputOps = "write"
		reqObj1.InputKey = key + strconv.Itoa(i)
		reqObj1.InputValue = []byte(value + strconv.Itoa(i))
		reqobjs_write = append(reqobjs_write, reqObj1)

		reqObj2 := niovakvlib.NiovaKV{}
		reqObj2.InputOps = "read"
		reqObj2.InputKey = key + strconv.Itoa(i)
		reqobjs_read = append(reqobjs_read, reqObj2)
	}

	//Send write request
	go printProgress("write", n)
	addr, port := getServerAddr(true)
	for j := 0; j < n; j++ {
		addr, port := getServerAddr(false)
                go sendReq(&reqobjs_write[j], addr, port, true)
		if serial == "yes" {
                   w.Wait()
                }
	}
	w.Wait()
	log.Info("No of writes sent, no of failures : ", total, failures)
	total = 0
	failures = 0

	//send read request
	go printProgress("read", n)
	for j := n - 1; j >= 0; j-- {
		addr, port = getServerAddr(false)
                go sendReq(&reqobjs_read[j], addr, port, false)
		if serial == "yes" {
                   w.Wait()
                }
	}

	//Wait till all reads are completed
        w.Wait()

	//Bar to show how many reads have been completed
        log.Info("No of reads sent, no of failures : ", total, failures)

	//Find avg response time
	Writesum := 0
	Readsum := 0
	for _, ops := range operationMetaWriteObjs {
		Writesum += int(ops.TimeDuration.Milliseconds())
	}
	for _, ops := range operationMetaReadObjs {
		Readsum += int(ops.TimeDuration.Milliseconds())
	}
	log.Info("Avg write response time : ", Writesum/n, " milli sec")
	log.Info("Avg read response time : ", Readsum/n, " milli sec")
	log.Info("----END OF LOG---")
	toJson := make(map[string][]opData)
	toJson["write"] = operationMetaWriteObjs
	toJson["read"] = operationMetaReadObjs
	file, _ := json.MarshalIndent(toJson, "", " ")
	_ = ioutil.WriteFile(resultFile+".json", file, 0644)

}
