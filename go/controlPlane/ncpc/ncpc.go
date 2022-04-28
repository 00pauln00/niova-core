package main

import (
	"bytes"
	serviceDiscovery "common/clientAPI"
	"common/requestResponseLib"
	compressionLib "common/specificCompressionLib"
	"encoding/gob"
	"encoding/json"
	"flag"
	"fmt"
	uuid "github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	maps "golang.org/x/exp/maps"
	"io/ioutil"
	"math/rand"
	PumiceDBCommon "niova/go-pumicedb-lib/common"
	"os"
	"reflect"
	"strconv"
	"hash/crc32"
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
}

type request struct {
	Opcode    string    `json:"Operation"`
	Key       string    `json:"Key"`
	Value     []byte    `json:"Value"`
	Timestamp time.Time `json:"Request_timestamp"`
}

type response struct {
	Status        int       `json:"Status"`
	ResponseValue []byte    `json:"Response"`
	validate      bool      `json:"validate"`
	Timestamp     time.Time `json:"Response_timestamp"`
}
type opData struct {
	RequestData  request       `json:"Request"`
	ResponseData response      `json:"Response"`
	TimeDuration time.Duration `json:"Req_resolved_time"`
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

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int, r *rand.Rand) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[r.Intn(len(letters))]
	}
	return string(b)
}

func generateVdevRange(count int, seed int64) map[string]string {
	kvMap := make(map[string]string)
	r := rand.New(rand.NewSource(seed))
	r1 := rand.New(rand.NewSource(seed))

	//Vdev
	noVdev := int64(float64(count) * float64(0.5))
	noUUID := r.Int63n(noVdev)
	for i := int64(0); i < noUUID; i++ {
		randUUID, _ := uuid.NewRandomFromReader(r)
		prefix := "v." + randUUID.String()
		kvMap[prefix] = randSeq(4, r1)

		noChunck := r.Int31n(5)
		Cprefix := prefix + ".c"
		for j := int32(0); j < noChunck; j++ {
			randUUID, _ := uuid.NewRandomFromReader(r)
			Chunckprefix := Cprefix + strconv.Itoa(int(j)) + "." + randUUID.String()
			kvMap[Chunckprefix] = randSeq(4, r1)
		}

		noSeq := r.Int31n(5)
		Sprefix := prefix + ".s"
		for k := int32(0); k < noSeq; k++ {
			SeqPrefix := Sprefix + strconv.Itoa(int(k))
			kvMap[SeqPrefix] = randSeq(4, r1)
		}
	}

	//NISD
	noNISD := int64(float64(count) * float64(0.5))
	noUUID = r.Int63n(noNISD)
	for i := int64(0); i < noUUID; i++ {
		randUUID, _ := uuid.NewRandomFromReader(r)
		prefix := "nisd." + randUUID.String()

		noNode := r.Int31n(5)
		nodePrefix := prefix + "."
		for j := int32(0); j < noNode; j++ {
			randUUID, _ := uuid.NewRandomFromReader(r)
			partNodePrefix := nodePrefix + randUUID.String()
			kvMap[partNodePrefix] = randSeq(4, r1)
		}

		configInfo := prefix + ".Config-Info"
		kvMap[configInfo] = randSeq(4, r1)
	}
	return kvMap
}

func filterKVPrefix(kvMap map[string]string, prefix string) map[string]string {
	resultantMap := make(map[string]string)
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
	flag.StringVar(&handler.configPath, "c", "./gossipNodes", "gossip nodes file path")
	flag.StringVar(&handler.logPath, "l", "/tmp/temp.log", "Log path")
	flag.StringVar(&handler.operation, "o", "NULL", "Specify the opeation to perform")
	flag.StringVar(&handler.resultFile, "r", "operation", "Path along with file name for the result file")
	flag.StringVar(&handler.rncui, "u", uuid.New().String()+":0:0:0:0", "RNCUI for request")
	flag.IntVar(&handler.count, "n", 1000, "Range write count")
	flag.IntVar(&handler.seed, "s", 10, "Seed value")
	flag.Parse()
}

//Write to Json
func (cli *clientHandler) write2Json(toJson map[string][]opData) {
	file, _ := json.MarshalIndent(toJson, "", " ")
	_ = ioutil.WriteFile(cli.resultFile+".json", file, 0644)
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
	var passNext bool
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
		passNext = true
		fallthrough
	case "read":
		requestObj.Key = clientObj.requestKey
		requestObj.Operation = clientObj.operation
		var requestByte bytes.Buffer
		enc := gob.NewEncoder(&requestByte)
		enc.Encode(requestObj)
		//Send the write
		responseBytes := clientObj.clientAPIObj.Request(requestByte.Bytes(), "", passNext)
		dec := gob.NewDecoder(bytes.NewBuffer(responseBytes))
		err = dec.Decode(&responseObj)
		fmt.Println(responseObj.ResultMap)
		//Creation of output json
		sendTime := time.Now()
		requestMeta := request{
			Opcode:    requestObj.Operation,
			Key:       requestObj.Key,
			Value:     responseObj.Value,
			Timestamp: sendTime,
		}

		responseMeta := response{
			Timestamp:     time.Now(),
			Status:        responseObj.Status,
			ResponseValue: responseObj.Value,
		}

		operationObj := opData{
			RequestData:  requestMeta,
			ResponseData: responseMeta,
			TimeDuration: responseMeta.Timestamp.Sub(requestMeta.Timestamp),
		}

		clientObj.operationMetaObjs = append(clientObj.operationMetaObjs, operationObj)
		if passNext {
			toJson["write"] = clientObj.operationMetaObjs
		} else {
			toJson["read"] = clientObj.operationMetaObjs
		}
		clientObj.write2Json(toJson)

	case "rangeWrite":
		kvMap := generateVdevRange(clientObj.count, int64(clientObj.seed))
		var wg sync.WaitGroup
		for key, _ := range kvMap {
			wg.Add(1)
			go func(key string, val []byte) {
				defer wg.Done()
				startTime := time.Now() //Start time
				request := requestResponseLib.KVRequest{}
				response := requestResponseLib.KVResponse{}
				var requestByte bytes.Buffer
				request.Operation = "write"
				request.Key = key
				request.Value = val
				request.Rncui = uuid.New().String() + ":0:0:0:0"
				enc := gob.NewEncoder(&requestByte)
				enc.Encode(request)
				beforeSendTime := time.Now() //Before time

				//Send the write request
				responseBytes := clientObj.clientAPIObj.Request(requestByte.Bytes(), "", true)

				recvResponseTime := time.Now() //Response time
				//Decode the request
				dec := gob.NewDecoder(bytes.NewBuffer(responseBytes))
				err = dec.Decode(&response)
				log.Info(key, response.Status)

				//Get hash for bytes
                                checkSum := crc32.ChecksumIEEE(requestByte.Bytes())
				elapsedTime := recvResponseTime.Sub(beforeSendTime)
				log.Info(";Request time ;",checkSum ,";",startTime, ";", beforeSendTime, ";",recvResponseTime, ";",elapsedTime)
			}(key, []byte(kvMap[key]))
		}
		wg.Wait()

	case "range":
		var Prefix, Key, Operation string
		Prefix = clientObj.requestKey
		Key = clientObj.requestKey
		Operation = "rangeRead"
		//Keep calling range request till ContinueRead is true
		resultMap := make(map[string]string)
		var count int
		for {
			rangeResponseObj := requestResponseLib.KVResponse{}
			requestObj.Prefix = Prefix
			requestObj.Key = Key
			requestObj.Operation = Operation
			var requestByte bytes.Buffer
			enc := gob.NewEncoder(&requestByte)
			enc.Encode(requestObj)

			//Send the range request
			responseBytes := clientObj.clientAPIObj.Request(requestByte.Bytes(), "", false)
			dec := gob.NewDecoder(bytes.NewBuffer(responseBytes))
			err = dec.Decode(&rangeResponseObj)
			if err != nil {
				log.Error(err)
				break
			}
			maps.Copy(resultMap, rangeResponseObj.ResultMap)

			for key, value := range rangeResponseObj.ResultMap {
				fmt.Println(key, " : ", value)
			}

			count += 1
			if !rangeResponseObj.ContinueRead {
				break
			}
			Prefix = clientObj.requestKey
			Key = rangeResponseObj.Key
		}
		genKVMap := generateVdevRange(clientObj.count, int64(clientObj.seed))
		filteredMap := filterKVPrefix(genKVMap, clientObj.requestKey)
		compare := reflect.DeepEqual(resultMap, filteredMap)
		if compare {
			fmt.Println("Got expected output")
		} else {
			fmt.Println("Range read failure")
		}
		fmt.Println("Called range query", count, "times")

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
		_ = ioutil.WriteFile(clientObj.resultFile+".json", fileData, 0644)
	}

	//clientObj.clientAPIObj.DumpIntoJson("./execution_summary.json")

}
