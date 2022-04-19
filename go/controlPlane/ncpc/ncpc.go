package main

import (
	"bytes"
	serviceDiscovery "common/clientAPI"
	"common/requestResponseLib"
	compressionLib "common/specificCompressionLib"
	"encoding/gob"
	"encoding/json"
	"flag"
	"math/rand"
	"fmt"
	"io/ioutil"
	PumiceDBCommon "niova/go-pumicedb-lib/common"
	"os"
	"strings"
	"time"
	"strconv"
	uuid "github.com/google/uuid"
	log "github.com/sirupsen/logrus"
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
	lastKey           string
	operationMetaObjs []opData //For filling json data
	clientAPIObj      serviceDiscovery.ServiceDiscoveryHandler
}

type request struct {
	Opcode    string            `json:"Operation"`
	Key       string            `json:"Key"`
	Value     []byte            `json:"Value"`
	Timestamp time.Time         `json:"Request_timestamp"`
}

type response struct {
	Status        int               `json:"Status"`
	ResponseValue []byte            `json:"Response"`
	validate      bool              `json:"validate"`
	Timestamp     time.Time         `json:"Response_timestamp"`
}
type opData struct {
	RequestData  request       `json:"Request"`
	ResponseData response      `json:"Response"`
	TimeDuration time.Duration `json:"Req_resolved_time"`
}

type nisdData struct {
	UUID      string `json:"UUID"`
	Status    string `json:"Status"`
	WriteSize string `json:"WriteSize"`
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


func generateVdevRange(seed int64) map[string]string {
	kvMap := make(map[string]string)
	r := rand.New(rand.NewSource(seed))
	noUUID := r.Int63n(10)
	for i := int64(0); i < noUUID; i++ {
		randUUID, _ := uuid.NewRandomFromReader(r)
		prefix := "v."+randUUID.String()
		kvMap[prefix] = "val1"

		noChunck := r.Int31n(5)
		Cprefix := prefix + ".c"
		for j := int32(0); j < noChunck; j++ {
			Chunckprefix := Cprefix + strconv.Itoa(int(j))
			kvMap[Chunckprefix] = "val2"
		}

		noSeq := rand.Int31n(5)
		Sprefix := prefix + ".s"
		for k := int32(0); k < noSeq; k++ {
			SeqPrefix := Sprefix + strconv.Itoa(int(k))
			kvMap[SeqPrefix] = "val3"
		}
	}
	return kvMap
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
				uuid, err := compressionLib.DecompressUUID(cuuid)
				if err == nil {
					CompressedStatus := value[0]

					//Decompress
					thisNISDData := nisdData{}
					thisNISDData.UUID = uuid
					log.Info("NISD Status : ", CompressedStatus)
					if string(CompressedStatus) == "1" {
						thisNISDData.Status = "Alive"
					} else {
						thisNISDData.Status = "Dead"
					}

					nisdDataMap[uuid] = thisNISDData
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
		fmt.Println(string(responseObj.Value))
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
		if write {
			toJson["write"] = clientObj.operationMetaObjs
		} else {
			toJson["read"] = clientObj.operationMetaObjs
		}
		clientObj.write2Json(toJson)

	case "rangeWrite":
		kvMap := generateVdevRange(10)
		requestObj.Operation = "write"
		for key, _ := range kvMap {
		        var requestByte bytes.Buffer
			requestObj.Key = key
			requestObj.Value = []byte(kvMap[key])
			requestObj.Rncui = uuid.New().String()+":0:0:0:0"
			enc := gob.NewEncoder(&requestByte)
			enc.Encode(requestObj)
			responseBytes := clientObj.clientAPIObj.Request(requestByte.Bytes(), "", true)
			dec := gob.NewDecoder(bytes.NewBuffer(responseBytes))
			err = dec.Decode(&responseObj)
			log.Info(key, responseObj.Status)
		}

	case "range":
		requestObj.Prefix = clientObj.requestKey
		requestObj.Key = clientObj.requestKey
		requestObj.Operation = clientObj.operation
		var requestByte bytes.Buffer
		enc := gob.NewEncoder(&requestByte)
		enc.Encode(requestObj)

		//Send the range request
		responseBytes := clientObj.clientAPIObj.Request(requestByte.Bytes(), "", false)
		dec := gob.NewDecoder(bytes.NewBuffer(responseBytes))
		err = dec.Decode(&responseObj)
		if err != nil {
			log.Error(err)
			break
		}

		for responseObj.ContinueRead {
			requestObj.Key = responseObj.Key
			enc.Encode(requestObj)
			//Send the range request
			responseBytes := clientObj.clientAPIObj.Request(requestByte.Bytes(), "", false)
			dec := gob.NewDecoder(bytes.NewBuffer(responseBytes))
			err = dec.Decode(&responseObj)
			fmt.Println(responseObj.Value)
			if err != nil {
				log.Error(err)
				break
			}
		}
		fmt.Println(responseObj.RangeMap)


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
		nisdDataMap := clientObj.getNISDInfo()
		file, _ := json.MarshalIndent(nisdDataMap, "", " ")
		_ = ioutil.WriteFile(clientObj.resultFile+".json", file, 0644)
	}

	//clientObj.clientAPIObj.DumpIntoJson("./execution_summary.json")

}
