package main

import (
	leaseClientLib "LeaseLib/leaseClient"
	"bytes"
	serviceDiscovery "common/clientAPI"
	leaseLib "common/leaseLib"
	"common/requestResponseLib"
	compressionLib "common/specificCompressionLib"
	"encoding/gob"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
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

	uuid "github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
	maps "golang.org/x/exp/maps"
)

type clientHandler struct {
	requestKey         string
	requestValue       string
	raftUUID           string
	addr               string
	port               string
	operation          string
	configPath         string
	logPath            string
	resultFile         string
	rncui              string
	rangeQuery         bool
	relaxedConsistency bool
	count              int
	seed               int
	lastKey            string
	operationMetaObjs  []opData //For filling json data
	clientAPIObj       serviceDiscovery.ServiceDiscoveryHandler
	seqNum             uint64
	valSize            int
	serviceRetry       int
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
	var vdevUUID []string
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
		randomNodeUUID := uuid.NewV4()
		nodeUUID = append(nodeUUID, randomNodeUUID.String())
		prefix := "node." + randomNodeUUID.String()

		//NISD-UUIDs
		for j := int64(0); j < noUUID; j++ {
			randUUID := uuid.NewV4()
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
	for _, node := range nodeUUID {
		for _, nisd := range nodeNisdMap[node] {
			prefix := "nisd." + nisd

			//Node-UUID
			kvMap[prefix+".Node-UUID"] = []byte(node)

			//Config-Info
			configInfo := prefix + ".Config-Info"
			kvMap[configInfo] = randSeq(valSize, r)

			//VDEV-UUID
			for j := int64(0); j < noUUID; j++ {
				randUUID := uuid.NewV4()
				partNodePrefix := prefix + "." + randUUID.String()
				kvMap[partNodePrefix] = randSeq(valSize, r)
				vdevUUID = append(vdevUUID, randUUID.String())
			}
		}
	}

	//Vdev
	/*
		User-Token
		Snapshots-Txn-Seqno
		Chunk-Number.Chunk-Component-UUID
	*/
	for i := int64(0); i < int64(len(vdevUUID)); i++ {
		prefix := "v." + vdevUUID[i]
		kvMap[prefix+".User-Token"] = randSeq(valSize, r)

		noChunck := count
		Cprefix := prefix + ".c"
		for j := int64(0); j < noChunck; j++ {
			randUUID := uuid.NewV4()
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
	flag.StringVar(&handler.requestKey, "k", "", "Key - For ReadRange pass '<prefix>*' e.g. : -k 'vdev.*'")
	flag.StringVar(&handler.addr, "a", "127.0.0.1", "Addr value")
	flag.StringVar(&handler.port, "p", "1999", "Port value")
	flag.StringVar(&handler.requestValue, "v", "", "Value")
	flag.StringVar(&handler.raftUUID, "ru", "", "RaftUUID of the cluster to be queried")
	flag.StringVar(&handler.configPath, "c", "./gossipNodes", "gossip nodes config file path")
	flag.StringVar(&handler.logPath, "l", "/tmp/temp.log", "Log path")
	flag.StringVar(&handler.operation, "o", "rw", "Specify the opeation to perform")
	flag.StringVar(&handler.resultFile, "j", "json_output", "Path along with file name for the resultant json file")
	flag.StringVar(&handler.rncui, "u", uuid.NewV4().String()+":0:0:0:0", "RNCUI for request / Lookout uuid")
	flag.IntVar(&handler.count, "n", 1, "Write number of key/value pairs per key type (Default 1 will write the passed key/value)")
	flag.BoolVar(&handler.relaxedConsistency, "r", false, "Set this flag if range could be performed with relaxed consistency")
	flag.IntVar(&handler.seed, "s", 10, "Seed value")
	flag.IntVar(&handler.valSize, "vs", 512, "Random value generation size")
	flag.Uint64Var(&handler.seqNum, "S", math.MaxUint64, "Sequence Number for read")
	flag.IntVar(&handler.serviceRetry, "sr", 1, "how many times you want to retry to pick the server if proxy is not available")
	flag.Parse()
}

//Write to Json
func (cli *clientHandler) write2Json(toJson interface{}) {
	file, err := json.MarshalIndent(toJson, "", " ")
	err = ioutil.WriteFile(cli.resultFile+".json", file, 0644)
	if err != nil {
		log.Error("Error in writing output to the file : ", err)
	}
}

//Converts map[string][]byte to map[string]string
func convMapToStr(map1 map[string][]byte) map[string]string {
	map2 := make(map[string]string)

	for k, v := range map1 {
		map2[k] = string(v)
	}

	return map2
}

func prepareOutput(status int, operation string, key string, value interface{}, seqNo uint64) *opData {
	requestMeta := request{
		Opcode: operation,
		Key:    key,
		Value:  value,
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
					thisNISDData.UUID, err = uuid.FromString(d_uuid)
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

func prepareKVRequest(key string, value []byte, rncui string, operation int, retBytes *bytes.Buffer) error {
	o := requestResponseLib.KVRequest{
		Operation: operation,
		Key:       key,
		Value:     value,
	}
	return PumiceDBCommon.PrepareAppPumiceRequest(o, rncui, retBytes)
}

func (clientObj *clientHandler) prepareLOInfoRequest(b *bytes.Buffer) error {
	//Request obj
	var o requestResponseLib.LookoutRequest
	var err error

	//Parse UUID
	o.UUID, err = uuid.FromString(clientObj.requestKey)
	if err != nil {
		log.Error("Invalid argument - key must be UUID")
		return err
	}
	o.Cmd = clientObj.requestValue

	enc := gob.NewEncoder(b)
	err = enc.Encode(o)
	if err != nil {
		log.Error("Encodng error : ", err)
	}
	return err
}

func (clientObj *clientHandler) write() {

	clientObj.operation = "write"
	kvMap := make(map[string][]byte)
	// Fill kvMap with key/val from user or generate keys/vals
	if clientObj.count > 1 {
		kvMap = generateVdevRange(int64(clientObj.count), int64(clientObj.seed), clientObj.valSize)
	} else {
		kvMap[clientObj.requestKey] = []byte(clientObj.requestValue)
	}

	var opStat *opData
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

			var rso requestResponseLib.KVResponse
			var rsb []byte

			err := func() error {

				//Fill the request object
				var rqb bytes.Buffer
				err := prepareKVRequest(key, val, uuid.NewV4().String()+":0:0:0:0", requestResponseLib.KV_WRITE, &rqb)
				if err != nil {
					log.Error("Error while preparing KV request : ", err)
					return err
				}

				//Send the write request
				rsb, err = clientObj.clientAPIObj.Request(rqb.Bytes(), "", true)
				if err != nil {
					log.Error("Error while sending the request : ", err)
					return err
				}

				//Decode the request
				dec := gob.NewDecoder(bytes.NewBuffer(rsb))
				err = dec.Decode(&rso)
				if err != nil {
					log.Error("Decoding error : ", err)
					return err
				}

				return nil
			}()

			// preparing appReqObj to write to jsonOutfile
			rqo := requestResponseLib.KVRequest{
				Key:       key,
				Value:     val,
				Operation: requestResponseLib.KV_WRITE,
			}

			//Request status filler
			if err != nil {
				opStat = prepareOutput(1, "write", rqo.Key, err.Error(), 0)
			} else {
				opStat = prepareOutput(rso.Status, "write", rqo.Key, string(rqo.Value), 0)
			}

			mut.Lock()
			clientObj.operationMetaObjs = append(clientObj.operationMetaObjs, *opStat)
			mut.Unlock()
		}(key, val)
	}
	wg.Wait()
	clientObj.write2Json(clientObj.operationMetaObjs)
}

func (clientObj *clientHandler) read() {
	var rso requestResponseLib.KVResponse

	clientObj.operation = "read"
	err := func() error {
		//Fill the request obj and encode it
		var rqb bytes.Buffer
		err := prepareKVRequest(clientObj.requestKey, []byte(""), "", requestResponseLib.KV_READ, &rqb)
		if err != nil {
			log.Error("Error while preparing KV request : ", err)
			return err
		}

		//Send the request
		rsb, err := clientObj.clientAPIObj.Request(rqb.Bytes(), "", false)
		if err != nil {
			log.Error("Error while sending the request : ", err)
			return err
		}

		//Decode the request
		dec := gob.NewDecoder(bytes.NewBuffer(rsb))
		err = dec.Decode(&rso)
		if err != nil {
			log.Error("Decoding error : ", err)
			return err
		}

		return nil
	}()

	if err == nil {
		opStat := prepareOutput(rso.Status, "read", rso.Key, string(rso.ResultMap[rso.Key]), 0)
		clientObj.operationMetaObjs = append(clientObj.operationMetaObjs, *opStat)
	} else {
		opStat := prepareOutput(1, "read", rso.Key, err.Error(), 0)
		clientObj.operationMetaObjs = append(clientObj.operationMetaObjs, *opStat)
	}

	clientObj.write2Json(clientObj.operationMetaObjs)
}

func (clientObj *clientHandler) rangeRead() {
	var prefix, key string
	var op int
	var err error
	var rqo requestResponseLib.KVRequest
	var seqNum uint64
	var opStat *opData

	clientObj.operation = "read"
	prefix = clientObj.requestKey[:len(clientObj.requestKey)-1]
	key = clientObj.requestKey[:len(clientObj.requestKey)-1]

	op = requestResponseLib.KV_RANGE_READ
	// get sequence number from arguments
	seqNum = clientObj.seqNum
	// Keep calling range request till ContinueRead is true
	resultMap := make(map[string][]byte)
	var count int

	rqo.Prefix = prefix
	rqo.Operation = op
	rqo.Consistent = !clientObj.relaxedConsistency
	for {
		var rso requestResponseLib.KVResponse
		var rqb bytes.Buffer
		var rsb []byte

		rqo.Key = key
		rqo.SeqNum = seqNum

		err = PumiceDBCommon.PrepareAppPumiceRequest(rqo, "", &rqb)
		if err != nil {
			log.Error("Pumice request creation error : ", err)
			break
		}

		//Send the range request
		rsb, err = clientObj.clientAPIObj.Request(rqb.Bytes(), "", false)
		if err != nil {
			log.Error("Error while sending request : ", err)
		}

		if len(rsb) == 0 {
			err = errors.New("Key not found")
			log.Error("Empty response : ", err)
			break
		}
		// decode the responseObj
		dec := gob.NewDecoder(bytes.NewBuffer(rsb))
		err = dec.Decode(&rso)
		if err != nil {
			log.Error("Decoding error : ", err)
			break
		}

		// copy result to global result variable
		maps.Copy(resultMap, rso.ResultMap)
		count += 1

		//Change sequence number and key for next iteration
		seqNum = rso.SeqNum
		key = rso.Key
		if !rso.ContinueRead {
			break
		}
	}

	//Fill the json
	if err == nil {
		sRMap := convMapToStr(resultMap)
		opStat = prepareOutput(0, "range", rqo.Key, sRMap, seqNum)
		clientObj.operationMetaObjs = append(clientObj.operationMetaObjs, *opStat)
		//Validate the range output
		fmt.Println("Generate the Data for read validation")
		gKVMap := generateVdevRange(int64(clientObj.count), int64(clientObj.seed), clientObj.valSize)

		// Get the expected data for read operation and compare against the output.
		tPrefix := clientObj.requestKey[:len(clientObj.requestKey)-1]
		fMap := filterKVPrefix(gKVMap, tPrefix)

		compare := reflect.DeepEqual(resultMap, fMap)
		if !compare {
			fmt.Println("Range verification read failure")
		}
		fmt.Println("The range query was completed in", count, "iterations")

	} else {
		opStat = prepareOutput(1, "range", rqo.Key, err.Error(), seqNum)
		clientObj.operationMetaObjs = append(clientObj.operationMetaObjs, *opStat)
	}

	clientObj.write2Json(clientObj.operationMetaObjs)
}

func (clientObj *clientHandler) processReadWriteReq() {

	//Wait till proxy is ready
	clientObj.waitServiceInit("PROXY")

	switch clientObj.operation {
	case "rw":
		clientObj.write()
		clientObj.read()
		break
	case "write":
		clientObj.write()
	case "read":
		if !isRangeRequest(clientObj.requestKey) {
			clientObj.read()
		} else {
			clientObj.rangeRead()
		}
	default:
		log.Error("Invalid operation type")
	}
}

func (clientObj *clientHandler) processConfig() {
	rsb, err := clientObj.clientAPIObj.GetPMDBServerConfig()
	log.Info("Response : ", string(rsb))
	if err != nil {
		log.Error("Unable to get the config data")
	}
	_ = ioutil.WriteFile(clientObj.resultFile+".json", rsb, 0644)
}

func (clientObj *clientHandler) processMembership() {
	toJson := clientObj.clientAPIObj.GetMembership()
	file, _ := json.MarshalIndent(toJson, "", " ")
	_ = ioutil.WriteFile(clientObj.resultFile+".json", file, 0644)
}

func (clientObj *clientHandler) processGeneral() {
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
}

func (clientObj *clientHandler) processNisd() {
	fmt.Printf("\033[2J")
	fmt.Printf("\033[2;0H")
	fmt.Println("NISD_UUID")
	fmt.Printf("\033[2;38H")
	fmt.Print("Status")
	fmt.Printf("\033[2;45H")
	fmt.Println("Parent_UUID(Lookout)")
	offset := 3
	for {
		lCounter := 0
		data := clientObj.clientAPIObj.GetMembership()
		for _, node := range data {
			if (node.Tags["Type"] == "LOOKOUT") && (node.Status == "alive") {
				for uuid, value := range node.Tags {
					if uuid != "Type" {
						currLine := offset + lCounter
						fmt.Print(uuid)
						fmt.Printf("\033[%d;38H", currLine)
						fmt.Print(strings.Split(value, "_")[0])
						fmt.Printf("\033[%d;45H", currLine)
						fmt.Println(node.Name)
						lCounter += 1
					}
				}
			}
		}
		time.Sleep(2 * time.Second)
		fmt.Printf("\033[3;0H")
		for i := 0; i < lCounter; i++ {
			fmt.Println("                                                       ")
		}
		fmt.Printf("\033[3;0H")
	}
}

func (clientObj *clientHandler) processGossip() {
	fileData, err := clientObj.clientAPIObj.GetPMDBServerConfig()
	if err != nil {
		log.Error("Error while getting pmdb server config data : ", err)
		return
	}
	f, _ := os.OpenFile(clientObj.resultFile+".json", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	f.WriteString(string(fileData))

	ioutil.WriteFile(clientObj.resultFile+".json", fileData, 0644)
}

func (clientObj *clientHandler) processProxyStat() {
	clientObj.clientAPIObj.ServerChooseAlgorithm = 2
	clientObj.clientAPIObj.UseSpecificServerName = clientObj.requestKey
	resBytes, err := clientObj.clientAPIObj.Request(nil, "/stat", false)
	if err != nil {
		log.Error("Error while sending request to proxy : ", err)
	}
	ioutil.WriteFile(clientObj.resultFile+".json", resBytes, 0644)
}

func (clientObj *clientHandler) processLookoutInfo() error {
	clientObj.clientAPIObj.ServerChooseAlgorithm = 2
	clientObj.clientAPIObj.UseSpecificServerName = clientObj.rncui

	var b bytes.Buffer
	var r []byte

	err := clientObj.prepareLOInfoRequest(&b)
	if err != nil {
		log.Error("Error while preparing lookout request")
		return err
	}

	r, err = clientObj.clientAPIObj.Request(b.Bytes(), "/v1/", false)
	if err != nil {
		log.Error("Error while sending request : ", err)
		return err
	}

	ioutil.WriteFile(clientObj.resultFile+".json", r, 0644)

	return err
}

func (clientObj *clientHandler) waitServiceInit(service string) {
	err := clientObj.clientAPIObj.TillReady(service, clientObj.serviceRetry)
	if err != nil {
		opStat := prepareOutput(-1, "setup", "", err.Error(), 0)
		clientObj.write2Json(opStat)
		log.Error(err)
		os.Exit(1)
	}
}

func (clientObj *clientHandler) initServiceDisHandler() {
	clientObj.clientAPIObj = serviceDiscovery.ServiceDiscoveryHandler{
		HTTPRetry: 10,
		SerfRetry: 5,
		RaftUUID:  clientObj.raftUUID,
	}
}

func (clientObj *clientHandler) prepareLeaseHandlers(leaseReqHandler *leaseClientLib.LeaseClientReqHandler) error {
	raft, err := uuid.FromString(clientObj.raftUUID)
	if err != nil {
		log.Error("Error getting raft UUID ", err)
		return err
	}

	leaseClientObj := leaseClientLib.LeaseClient{
		RaftUUID:            raft,
		ServiceDiscoveryObj: &clientObj.clientAPIObj,
	}

	leaseReqHandler.LeaseClientObj = &leaseClientObj
	return err
}

func getLeaseOperationType(op string) int {
	switch op {
	case "GetLease":
		return leaseLib.GET
	case "LookupLease":
		return leaseLib.LOOKUP
	case "RefreshLease":
		return leaseLib.REFRESH
	default:
		log.Error("Invalid Lease operation type: ", op)
		return -1
	}
}

func (clientObj *clientHandler) performLeaseReq(resource, client string) error {
	clientObj.clientAPIObj.TillReady("PROXY", clientObj.serviceRetry)

	op := getLeaseOperationType(clientObj.operation)

	var lrh leaseClientLib.LeaseClientReqHandler
	err := clientObj.prepareLeaseHandlers(&lrh)
	if err != nil {
		log.Error("Error while preparing lease handlers : ", err)
		return err
	}
	err = lrh.InitLeaseReq(client, resource, op)
	if err != nil {
		log.Error("error while initializing lease req : ", err)
		return err
	}
	err = lrh.LeaseOperationOverHTTP()
	if err != nil {
		log.Error("Error sending lease request : ", err)
		return err
	}

	clientObj.write2Json(lrh)

	return err
}

func isRangeRequest(requestKey string) bool {
	return requestKey[len(requestKey)-1:] == "*"
}

//Check if for single key write operation, value has been passed.
func isSingleWriteReqValid(cli *clientHandler) bool {
	if cli.operation == "write" && cli.count == 1 && cli.requestValue == "" {
		return false
	}

	return true
}

func main() {
	//Intialize client object
	clientObj := clientHandler{}

	//Get commandline parameters.
	clientObj.getCmdParams()

	flag.Usage = usage
	if flag.NFlag() == 0 || !isSingleWriteReqValid(&clientObj) {
		usage()
		os.Exit(-1)
	}

	//Create logger
	err := PumiceDBCommon.InitLogger(clientObj.logPath)
	if err != nil {
		log.Error("Error while initializing the logger  ", err)
	}

	//Init service discovery
	clientObj.initServiceDisHandler()

	stop := make(chan int)
	go func() {
		log.Info("Start Serf client")
		err := clientObj.clientAPIObj.StartClientAPI(stop, clientObj.configPath)
		if err != nil {
			opStat := prepareOutput(-1, "setup", "", err.Error(), 0)
			clientObj.write2Json(opStat)
			log.Error(err)
			os.Exit(1)
		}
	}()

	//Wait till client API Object is ready
	clientObj.waitServiceInit("")

	var passNext bool
	switch clientObj.operation {
	case "rw":
		fallthrough
	case "write":
		fallthrough
	case "read":
		clientObj.processReadWriteReq()

	case "config":
		clientObj.processConfig()

	case "membership":
		clientObj.processMembership()

	case "general":
		clientObj.processGeneral()

	case "nisd":
		clientObj.processNisd()

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
		clientObj.processGossip()

	case "ProxyStat":
		clientObj.processProxyStat()

	case "LookoutInfo":
		err := clientObj.processLookoutInfo()
		if err != nil {
			break
		}

	//Lease Operations
	case "GetLease":
		fallthrough
	case "LookupLease":
		fallthrough
	case "RefreshLease":
		err := clientObj.performLeaseReq(clientObj.requestKey, clientObj.requestValue)
		if err != nil {
			break
		}
	}
}
