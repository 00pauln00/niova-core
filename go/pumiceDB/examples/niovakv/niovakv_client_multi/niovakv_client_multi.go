package main

import (
	"encoding/json"
	"flag"
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
	atomic.AddInt32(requestSentCount, 1)
	requestMeta := request{
		Opcode:    req.InputOps,
		Key:       req.InputKey,
		Value:     string(req.InputValue),
		Sent_to:   addr + ":" + port,
		Timestamp: time.Now(),
	}
	var resp *niovakvlib.NiovaKVResponse
	log.Info("Send to addr : ",addr," ",port)
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
}

//Get any client addr
func getServerAddr(refresh bool) (string, string) {
	if refresh {
		ClientHandler.GetData(false)
	}
	//Get random addr
	log.Info(ClientHandler.Agents)
	randomIndex := rand.Intn(len(ClientHandler.Agents))
	node := ClientHandler.Agents[randomIndex]
	if node.Tags["Hport"]==""{
		return getServerAddr(true)
	}
	return node.Addr.String(), node.Tags["Hport"]
}

func main() {
	var count1, count3 int32
	count1 = 0
	count3 = 0
	requestSentCount = &count1
	timeoutCount = &count3
	//Get commandline parameters.
	getCmdParams()
	flag.Usage = usage
	flag.Parse()

	if flag.NFlag() == 0 {
		usage()
		os.Exit(-1)
	}

	//Create log file.
	err := PumiceDBCommon.InitLogger(logPath)
	if err != nil {
		log.Error("Error while initializing the logger  ", err)
	}

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

	//Send request
	addr, port := getServerAddr(true)
	for j := 0; j < n; j++ {
		addr, port := getServerAddr(true)
		if serial == "no" {
			w.Add(1)
			go func(index int) {
				sendReq(&reqobjs_write[index], addr, port, true)
				w.Done()
			}(j)
		} else {
			sendReq(&reqobjs_write[j], addr, port, true)
		}
	}

	//Wait till all write are completed
	log.Info("Waiting for writes to complete")
	w.Wait()
	log.Info("Writes completed")
	log.Info("No of request sent, no of time outs : ", count1, count3)
	count1 = 0
	count3 = 0

	for j := n - 1; j >= 0; j-- {
		addr, port = getServerAddr(true)
		if serial == "no" {
			w.Add(1)
			go func(index int) {
				sendReq(&reqobjs_read[index], addr, port, false)
				w.Done()
			}(j)
		} else {
			sendReq(&reqobjs_read[j], addr, port, false)
		}
	}

	//Wait till all reads are completed
	log.Info("Waiting for read to complete")
	w.Wait()
	log.Info("Reads completed")
	log.Info("No of request sent, no of time outs : ", count1, count3)

	//Find avg response time
	Writesum := 0
	Readsum := 0
	for _, ops := range operationMetaWriteObjs {
		Writesum += int(ops.TimeDuration.Seconds())
	}
	for _, ops := range operationMetaReadObjs {
		Readsum += int(ops.TimeDuration.Seconds())
	}
	log.Info("Avg write response time : ", Writesum/n, "sec")
	log.Info("Avg read response time : ", Readsum/n, "sec")
	log.Info("Avg response time : ", (Writesum+Readsum)/(2*n), "sec")
	toJson := make(map[string][]opData)
	toJson["write"] = operationMetaWriteObjs
	toJson["read"] = operationMetaReadObjs
	file, _ := json.MarshalIndent(toJson, "", " ")
	_ = ioutil.WriteFile(resultFile+".json", file, 0644)

}
