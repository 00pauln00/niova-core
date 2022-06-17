package main

import (
	"bytes"
	serviceDiscovery "common/clientAPI"
	"common/requestResponseLib"
	compressionLib "common/specificCompressionLib"
	"encoding/gob"
	"errors"
	"encoding/json"
	"flag"
	"fmt"
	uuid "github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	maps "golang.org/x/exp/maps"
	"io/ioutil"
	"math"
	"math/rand"
	PumiceDBCommon "niova/go-pumicedb-lib/common"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

type clientHandler struct {
	requestKey        string
	requestValue      string
	addr              string
	port              string
	operation         string
	configPath        string
	logPath           string
	resultFile        string
	rncui             string
	rangeQuery        bool
	count             int
	seed              int
	lastKey           string
	operationMetaObjs []opData //For filling json data
	clientAPIObj      serviceDiscovery.ServiceDiscoveryHandler
	seqNum            uint64
	valSize           int
}

type request struct {
	Opcode    string      `json:"Operation"`
	Key       string      `json:"Key"`
	Value     interface{} `json:"Value"`
	Timestamp time.Time   `json:"Request_timestamp"`
}

type response struct {
	Status         int         `json:"Status"`
	ResponseValue  interface{} `json:"Response"`
	SequenceNumber uint64      `json:"Sequence_number"`
	validate       bool        `json:"validate"`
	Timestamp      time.Time   `json:"Response_timestamp"`
}

type opData struct {
	RequestData  request       `json:"Request"`
	ResponseData response      `json:"Response"`
	TimeDuration time.Duration `json:"Req_resolved_time"`
}

type multiWriteStatus struct {
	Status int
	Value  interface{}
}

type nisdData struct {
	UUID      uuid.UUID `json:"UUID"`
	Status    string    `json:"Status"`
	WriteSize string    `json:"WriteSize"`
}

func usage() {
	flag.PrintDefaults()
	os.Exit(0)
}

func getKeyType(key string) string {
	switch key[:2] {
	case "vd":
		return "vdevKey"
	case "ni":
		return "nisdKey"
	case "no":
		return "nodeKey"
	}
	return ""
}

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randSeq(n int, r *rand.Rand) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[r.Intn(len(letters))]
	}
	return b
}

func generateVdevRange(count int64, seed int64, valSize int) map[string][]byte {
	kvMap := make(map[string][]byte)
	r := rand.New(rand.NewSource(seed))
	var nodeUUID []string
	nodeNisdMap := make(map[string][]string)
	//Node UUID
	/*
		FailureDomain
		Info
		State
		HostName
		NISD-UUIDs
	*/
	noUUID := count
	for i := int64(0); i < noUUID; i++ {
		randomNodeUUID, _ := uuid.NewRandomFromReader(r)
		nodeUUID = append(nodeUUID, randomNodeUUID.String())
		prefix := "node." + randomNodeUUID.String()
		//FDK

		//NISD-UUIDs
		for j := int64(0); j < noUUID; j++ {
			randUUID, _ := uuid.NewRandomFromReader(r)
			nodeNisdMap[randomNodeUUID.String()] = append(nodeNisdMap[randomNodeUUID.String()], randUUID.String())
		}
		kvMap[prefix+".NISD-UUIDs"], _ = json.Marshal(nodeNisdMap[randomNodeUUID.String()])

	}
	//NISD
	/*
		Node-UUID
		Config-Info
		Device-Type
		Device-Path
		Device-Status
		Device-Info
		Device-Size
		Provisioned-Size
		VDEV-UUID.Chunk-Number.Chunk-Component-UUID
	*/
	for node, nisds := range nodeNisdMap {
		for _, nisd := range nisds {
			prefix := "nisd." + nisd

			//Node-UUID
			kvMap[prefix+".Node-UUID"] = []byte(node)

			//Config-Info
			configInfo := prefix + ".Config-Info"
			kvMap[configInfo] = randSeq(valSize, r)

			//VDEV-UUID
			for j := int64(0); j < noUUID; j++ {
				randUUID, _ := uuid.NewRandomFromReader(r)
				partNodePrefix := prefix +"."+randUUID.String()
				kvMap[partNodePrefix] = randSeq(valSize, r)
			}
		}
	}

	//Vdev
	/*
		User-Token
		Snapshots-Txn-Seqno
		Chunk-Number.Chunk-Component-UUID
	*/
	// FIXME Use the vdev uuid from above loop
	for i := int64(0); i < noUUID; i++ {
		randomVdevUUID, _ := uuid.NewRandomFromReader(r)
		prefix := "v." + randomVdevUUID.String()
		kvMap[prefix+".User-Token"] = randSeq(valSize, r)

		noChunck := count
		Cprefix := prefix + ".c"
		for j := int64(0); j < noChunck; j++ {
			randUUID, _ := uuid.NewRandomFromReader(r)
			Chunckprefix := Cprefix + strconv.Itoa(int(j)) + "." + randUUID.String()
			kvMap[Chunckprefix] = randSeq(valSize, r)
		}
	}
	return kvMap
}

func filterKVPrefix(kvMap map[string][]byte, prefix string) map[string][]byte {
	resultantMap := make(map[string][]byte)
	for key, value := range kvMap {
		if strings.HasPrefix(key, prefix) {
			resultantMap[key] = value
		}
	}

	return resultantMap
}

//Function to get command line parameters
func (handler *clientHandler) getCmdParams() {
	flag.StringVar(&handler.requestKey, "k", "Key", "Key")
	flag.StringVar(&handler.addr, "a", "127.0.0.1", "Addr value")
	flag.StringVar(&handler.port, "p", "1999", "Port value")
	flag.StringVar(&handler.requestValue, "v", "NULL", "Value")
	flag.StringVar(&handler.configPath, "c", "./gossipNodes", "gossip nodes config file path")
	flag.StringVar(&handler.logPath, "l", "/tmp/temp.log", "Log path")
	flag.StringVar(&handler.operation, "o", "rw", "Specify the opeation to perform")
	flag.StringVar(&handler.resultFile, "j", "json_output", "Path along with file name for the resultant json file")
	flag.StringVar(&handler.rncui, "u", uuid.New().String()+":0:0:0:0", "RNCUI for request / Lookout uuid")
	flag.IntVar(&handler.count, "n", 1, "Number of key-value write count")
	flag.IntVar(&handler.seed, "s", 10, "Seed value")
	flag.IntVar(&handler.valSize, "vs", 512, "Random value generation size")
	flag.Uint64Var(&handler.seqNum, "S", math.MaxUint64, "Sequence Number for read")
	flag.Parse()
}

//Write to Json
func (cli *clientHandler) write2Json(toJson interface{}) {
	file, _ := json.MarshalIndent(toJson, "", " ")
	_ = ioutil.WriteFile(cli.resultFile+".json", file, 0644)
}

func fillOperationData(status int, operation string, key string, value interface{}, seqNo uint64) *opData {
	requestMeta := request{
		Opcode:    operation,
		Key:       key,
		Value:     value,
	}

	responseMeta := response{
		SequenceNumber: seqNo,
		Status:         status,
		ResponseValue:  value,
	}

	operationObj := opData{
		RequestData:  requestMeta,
		ResponseData: responseMeta,
	}
	return &operationObj
}

func (cli *clientHandler) getNISDInfo() map[string]nisdData {
	data := cli.clientAPIObj.GetMembership()
	nisdDataMap := make(map[string]nisdData)
	for _, node := range data {
		if (node.Tags["Type"] == "LOOKOUT") && (node.Status == "alive") {
			for cuuid, value := range node.Tags {
				d_uuid, err := compressionLib.DecompressUUID(cuuid)
				if err == nil {
					CompressedStatus := value[0]
					//Decompress
					thisNISDData := nisdData{}
					thisNISDData.UUID, err = uuid.Parse(d_uuid)
					if err != nil {
						log.Error(err)
					}
					if string(CompressedStatus) == "1" {
						thisNISDData.Status = "Alive"
					} else {
						thisNISDData.Status = "Dead"
					}

					nisdDataMap[d_uuid] = thisNISDData
				}
			}
		}
	}
	return nisdDataMap
}


func (clientObj *clientHandler) write() {
	kvMap := make(map[string][]byte)
	// Fill kvMap with key/val from user or generate keys/vals
	if clientObj.count > 1 {
		kvMap = generateVdevRange(int64(clientObj.count), int64(clientObj.seed), clientObj.valSize)
	} else {
		kvMap[clientObj.requestKey] = []byte(clientObj.requestValue)
	}
	operationStatSlice := make(map[string]*multiWriteStatus)
	var operationStat interface{}
	var mut sync.Mutex
	var wg sync.WaitGroup
	// Create a int channel of fixed size to enqueue max requests
	requestLimiter := make(chan int, 100)
	for key, val := range kvMap {
		wg.Add(1)
		requestLimiter <- 1
		go func(key string, val []byte) {
			defer func() {
				wg.Done()
				<-requestLimiter
			}()
			var requestObj requestResponseLib.KVRequest
			var responseObj requestResponseLib.KVResponse
			var requestByte bytes.Buffer

			//Fill the request object
			requestObj.Operation = "write"
			requestObj.Key = key
			requestObj.Value = val
			requestObj.Rncui = uuid.New().String() + ":0:0:0:0"
			enc := gob.NewEncoder(&requestByte)
			err := enc.Encode(requestObj)
			if err != nil {
				log.Error("Encoding error : ", err)
				return
			}

			//Send the write request
			responseBytes := clientObj.clientAPIObj.Request(requestByte.Bytes(), "", true)
			//Decode the request
			dec := gob.NewDecoder(bytes.NewBuffer(responseBytes))
			err = dec.Decode(&responseObj)
			if err != nil {
				log.Error("Decoding error : ", err)
				return
			}

			//Request status filler
			if clientObj.count == 1 {
				operationStat = fillOperationData(responseObj.Status, "write", requestObj.Key, string(requestObj.Value), 0)
			} else {
				operationStatMulti := multiWriteStatus{
					Status: responseObj.Status,
					Value:  string(val),
				}
				mut.Lock()
				operationStatSlice[key] = &operationStatMulti
				operationStat = operationStatSlice
				mut.Unlock()
			}
		}(key, val)
	}
	wg.Wait()
	clientObj.write2Json(operationStat)
}

func (clientObj *clientHandler) read() {
	var requestObj requestResponseLib.KVRequest
	var responseObj requestResponseLib.KVResponse

	requestObj.Key = clientObj.requestKey
	requestObj.Operation = clientObj.operation
	var requestByte bytes.Buffer
	enc := gob.NewEncoder(&requestByte)
	err := enc.Encode(requestObj)
	if err != nil {
		log.Error("Encoding error : ", err)
	}

	//Send the request
	responseBytes := clientObj.clientAPIObj.Request(requestByte.Bytes(), "", false)

	//Decode the request
	dec := gob.NewDecoder(bytes.NewBuffer(responseBytes))
	err = dec.Decode(&responseObj)
	if err != nil {
		log.Error("Decoding error : ", err)
	}

	operationStat := fillOperationData(responseObj.Status, "read", responseObj.Key, responseObj.ResultMap[responseObj.Key], 0)
	clientObj.write2Json(operationStat)
}

func (clientObj *clientHandler) rangeRead() {
	var Prefix, Key, Operation string
	var reqStatus error
	var requestObj requestResponseLib.KVRequest

	Prefix = clientObj.requestKey[:len(clientObj.requestKey)-1]
	Key = clientObj.requestKey[:len(clientObj.requestKey)-1]

	Operation = "rangeRead"
	// get sequence number from arguments
	seqNum := clientObj.seqNum
	// Keep calling range request till ContinueRead is true
	resultMap := make(map[string]string)
	var count int
	for {
		rangeResponseObj := requestResponseLib.KVResponse{}
		requestObj.Prefix = Prefix
		requestObj.Key = Key
		requestObj.Operation = Operation
		requestObj.SeqNum = seqNum
		var requestByte bytes.Buffer

		// encode the requestObj
		enc := gob.NewEncoder(&requestByte)
		err := enc.Encode(requestObj)
		if err != nil {
			reqStatus = err
			log.Error("Encoding error : ", err)
			break
		}

		//Send the range request
		responseBytes := clientObj.clientAPIObj.Request(requestByte.Bytes(), "", false)
		if len(responseBytes) == 0 {
			reqStatus = errors.New("Key not found")
			log.Error(reqStatus)
			break
		}
		// decode the responseObj
		dec := gob.NewDecoder(bytes.NewBuffer(responseBytes))
		err = dec.Decode(&rangeResponseObj)
		if err != nil {
			reqStatus = err
			log.Error("Decoding error : ", err)
			break
		}
		// copy result to global result variable
		maps.Copy(resultMap, rangeResponseObj.ResultMap)
		count += 1
		seqNum = rangeResponseObj.SeqNum
		if !rangeResponseObj.ContinueRead {
			break
		}
		// set key and seqNum for next iteration of range request
		Key = rangeResponseObj.Key
	}
	//Get status from response
	operationStat := fillOperationData(0, "range", requestObj.Key, resultMap, seqNum)
	clientObj.write2Json(operationStat)
	// FIXME Failing
	if reqStatus == nil {
		fmt.Println("Generate the Data for read validation")
		genKVMap := generateVdevRange(int64(clientObj.count), int64(clientObj.seed), clientObj.valSize)

		// Get the expected data for read operation and compare against the output.
		tPrefix := clientObj.requestKey[:len(clientObj.requestKey)-1]
		filteredMap := filterKVPrefix(genKVMap, tPrefix)

		compare := reflect.DeepEqual(resultMap, filteredMap)
		if !compare {
			fmt.Println("Range verification read failure")
		}
		fmt.Println("The range query was completed in", count, "iterations")
	}
}

func isRangeRequest(requestKey string) bool {
	return requestKey[len(requestKey)-1:] == "*"
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

	//Init service discovery
	clientObj.clientAPIObj = serviceDiscovery.ServiceDiscoveryHandler{
		HTTPRetry: 10,
		SerfRetry: 5,
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

	var passNext bool
	switch clientObj.operation {
	case "rw":
		log.Info("Defaulting to write and read")
		clientObj.operation = "write"
		clientObj.write()
		clientObj.operation = "read"
		clientObj.read()

	case "write":
		clientObj.write()

	case "read":
		if !isRangeRequest(clientObj.requestKey) {
			clientObj.read()
		} else {
			clientObj.rangeRead()
		}

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

	case "Gossip":
		passNext = true

	case "NISDGossip":
		nisdDataMap := clientObj.getNISDInfo()
		fileData, _ := json.MarshalIndent(nisdDataMap, "", " ")
		ioutil.WriteFile(clientObj.resultFile+".json", fileData, 0644)
		if !passNext {
			break
		}
		fallthrough

	case "PMDBGossip":
		fileData, err := clientObj.clientAPIObj.GetPMDBServerConfig()
		if err != nil {
			log.Error("Error while getting pmdb server config data : ", err)
			break
		}
		if passNext {
			f, _ := os.OpenFile(clientObj.resultFile+".json", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
			f.WriteString(string(fileData))
			break
		}
		ioutil.WriteFile(clientObj.resultFile+".json", fileData, 0644)

	case "ProxyStat":
                clientObj.clientAPIObj.ServerChooseAlgorithm = 2
                clientObj.clientAPIObj.UseSpecificServerName = clientObj.requestKey
                responseBytes := clientObj.clientAPIObj.Request(nil, "/stat", false)
                ioutil.WriteFile(clientObj.resultFile+".json", responseBytes, 0644)

        case "LookoutInfo":
                clientObj.clientAPIObj.ServerChooseAlgorithm = 2
                clientObj.clientAPIObj.UseSpecificServerName = clientObj.rncui
                //Request obj
                var requestObj requestResponseLib.LookoutRequest

                //Parse UUID
                requestObj.NISD, _ = uuid.Parse(clientObj.requestKey)
                requestObj.Cmd = clientObj.requestValue
                var requestByte bytes.Buffer
                enc := gob.NewEncoder(&requestByte)
                err := enc.Encode(requestObj)
                if err != nil {
                        log.Info("Encoding error")
                }
                responseBytes := clientObj.clientAPIObj.Request(requestByte.Bytes(), "/v1/", false)
                clientObj.write2Json(responseBytes)
	}

	//clientObj.clientAPIObj.DumpIntoJson("./execution_summary.json")

}
