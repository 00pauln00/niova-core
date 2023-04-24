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
	"strconv"
	"strings"
	"sync"
	"time"

	uuid "github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
	maps "golang.org/x/exp/maps"
)

type clientReq struct {
	Request requestResponseLib.KVRequest
	Response requestResponseLib.KVResponse
}

type clientHandler struct {
	requestKey		   string
	requestValue	   string
	clientReqArr	   []clientReq
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

func (co *clientHandler)appendReq(kvArr *[]clientReq, key string, value []byte) {
	creq := clientReq{}
	creq.Request.Key = key
	creq.Request.Value = value

	*kvArr = append(*kvArr, creq)
}

// dummy function to mock user filling up multiple req
func (co *clientHandler)generateVdevRange() []clientReq {

	var kvArr []clientReq
	r := rand.New(rand.NewSource(int64(co.seed)))
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
	noUUID := co.count
	for i := int64(0); i < int64(noUUID); i++ {
		randomNodeUUID := uuid.NewV4()
		nodeUUID = append(nodeUUID, randomNodeUUID.String())
		prefix := "node." + randomNodeUUID.String()

		//NISD-UUIDs
		for j := int64(0); j < int64(noUUID); j++ {
			randUUID := uuid.NewV4()
			nodeNisdMap[randomNodeUUID.String()] = append(nodeNisdMap[randomNodeUUID.String()], randUUID.String())
		}

		nval, _ := json.Marshal(nodeNisdMap[randomNodeUUID.String()])
		co.appendReq(&kvArr, prefix+".NISD-UUIDs", nval)
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
			randomNodeUUID := uuid.NewV4()

			//Node-UUID
			co.appendReq(&kvArr, prefix+".Node-UUID", []byte(node))

			nval,_ := json.Marshal(nodeNisdMap[randomNodeUUID.String()])
			co.appendReq(&kvArr, prefix+".NISD-UUIDs", nval)

			//Config-Info
			co.appendReq(&kvArr, prefix + ".Config-Info", randSeq(co.valSize, r))

			//VDEV-UUID
			for j := int64(0); j < int64(noUUID); j++ {
				randUUID := uuid.NewV4()
				partNodePrefix := prefix + "." + randUUID.String()
				co.appendReq(&kvArr, partNodePrefix, randSeq(co.valSize, r))
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
		co.appendReq(&kvArr, prefix+".User-Token", randSeq(co.valSize, r))

		noChunck := co.count
		Cprefix := prefix + ".c"
		for j := int64(0); j < int64(noChunck); j++ {
			randUUID := uuid.NewV4()
			Chunckprefix := Cprefix + strconv.Itoa(int(j)) + "." + randUUID.String()
			co.appendReq(&kvArr, Chunckprefix, randSeq(co.valSize, r))
		}
	}
	return kvArr
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

func (cli *clientHandler) complete(data []byte) error {
	err := ioutil.WriteFile(cli.resultFile+".json", data, 0644)
	if err != nil {
		log.Error("Error in writing output to the file : ", err)
	}
	return err
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
	o.Cmd = string(clientObj.requestValue)

	enc := gob.NewEncoder(b)
	err = enc.Encode(o)
	if err != nil {
		log.Error("Encodng error : ", err)
	}
	return err
}

func (co *clientHandler) prepNSendReq(rncui string, isWrite bool, itr int) error {

	var rqb bytes.Buffer
	err := PumiceDBCommon.PrepareAppPumiceRequest(co.clientReqArr[itr].Request,
					rncui, &rqb)
	if err != nil {
		return err
	}

	//Send the request
	rsb, err := co.clientAPIObj.Request(rqb.Bytes(), "", isWrite)
	if err != nil {
		return err
	}

	//Decode the response to get the status of the operation. 
	res := &co.clientReqArr[itr].Response
	dec := gob.NewDecoder(bytes.NewBuffer(rsb))
	return dec.Decode(res)
}

func (co *clientHandler) write(wresult bool) ([]byte, error) {

	co.operation = "write"

	var wg sync.WaitGroup
	var err error
	// Create a int channel of fixed size to enqueue max requests
	requestLimiter := make(chan int, 100)

	// iterate over req and res, while performing reqs
	for i := 0; i < len(co.clientReqArr); i++ {
		wg.Add(1)
		requestLimiter <- 1
		co.clientReqArr[i].Request.Operation = requestResponseLib.KV_WRITE
		go func(itr int) {
			defer func() {
				wg.Done()
				<-requestLimiter
			}()

			err = func() error {
				err := co.prepNSendReq(uuid.NewV4().String()+":0:0:0:0", true, itr)
				return err
			}()
			if err != nil {
				return
			}
		}(i)
	}
	wg.Wait()
	file, err := json.MarshalIndent(co.clientReqArr, "", " ")
	if err != nil {
		log.Error("Failed to json.MarshalIndent cli.clientReqArr")
	}
	//If calling function asked to write the result immediately
	if wresult {
		err = ioutil.WriteFile(co.resultFile+".json", file, 0644)
		if err != nil {
			log.Error("Error in writing output to the file : ", err)
		}
		return nil, err
	}
	//else return the result byte array
	return json.MarshalIndent(co.clientReqArr, "", " ")
}

func (co *clientHandler) read() ([]byte, error) {

	//read single key passed from cmdline.
	creq := clientReq{}
	creq.Request.Operation = requestResponseLib.KV_READ
	creq.Request.Key = co.requestKey
	creq.Request.Value = []byte("")

	co.clientReqArr = append(co.clientReqArr, creq)

	co.operation = "read"
	err := func() error {
		return co.prepNSendReq("", false, 0)
	}()

	if err != nil {
		return nil, err
	}
	return json.MarshalIndent(co.clientReqArr, "", " ")
}

func (co *clientHandler) rangeRead() ([]byte, error) {
	var prefix, key string
	var op int
	var err error
	var seqNum uint64

	co.operation = "read"

	prefix = co.requestKey[:len(co.requestKey)-1]
	key = co.requestKey[:len(co.requestKey)-1]

	op = requestResponseLib.KV_RANGE_READ
	// get sequence number from arguments
	seqNum = co.seqNum
	// Keep calling range request till ContinueRead is true

	creq := clientReq{}
	creq.Request.Prefix = prefix
	creq.Request.Operation = op
	creq.Request.Consistent = !co.relaxedConsistency
	creq.Request.Key = key
	resultMap := make(map[string][]byte)
	for {
		var rqb bytes.Buffer
		var rsb []byte

		creq.Request.Key = key
		creq.Request.SeqNum = seqNum

		rso := &creq.Response
		err = PumiceDBCommon.PrepareAppPumiceRequest(creq.Request, "", &rqb)
		if err != nil {
			log.Error("Pumice request creation error : ", err)
			break
		}

		//Send the range request
		rsb, err = co.clientAPIObj.Request(rqb.Bytes(), "", false)
		if err != nil {
			log.Error("Error while sending request : ", err)
		}

		if len(rsb) == 0 {
			err = errors.New("Key not found")
			log.Error("Empty response : ", err)
			rso.Status = -1
			rso.Key = key
			break
		}
		// decode the responseObj
		dec := gob.NewDecoder(bytes.NewBuffer(rsb))
		err = dec.Decode(rso)
		if err != nil {
			log.Error("Decoding error : ", err)
			break
		}

		// copy result to global result variable
		maps.Copy(resultMap, rso.ResultMap)
		//Change sequence number and key for next iteration
		seqNum = rso.SeqNum
		key = rso.Key
		if !rso.ContinueRead {
			break
		}
	}
	co.clientReqArr = append(co.clientReqArr, creq) 
	maps.Clear(co.clientReqArr[0].Response.ResultMap)
	maps.Copy(co.clientReqArr[0].Response.ResultMap, resultMap)

	return json.MarshalIndent(co.clientReqArr, "", " ")
}

// check and fill request map acc to req count
func (clientObj *clientHandler) prepWriteReq(rArr []clientReq) {
	clientObj.clientReqArr = rArr
}

func (clientObj *clientHandler) getKVArray() []clientReq{
	var rArr []clientReq
	if clientObj.requestKey == "" && clientObj.requestValue == "" {
		rArr = clientObj.generateVdevRange()
	} else {
		creq := clientReq{}
		creq.Request.Key = clientObj.requestKey
		creq.Request.Value = []byte(clientObj.requestValue)
		rArr = append(clientObj.clientReqArr, creq)
	}
	return rArr
}

func (clientObj *clientHandler) processReadWriteReq(rArr []clientReq) ([]byte, error) {

	//Wait till proxy is ready
	err := clientObj.waitServiceInit("PROXY")
	if err != nil {
		return nil, err
	}

	var data []byte
	switch clientObj.operation {
	case "rw":
		clientObj.prepWriteReq(rArr)
		data, err = clientObj.write(true)
		if err == nil {
			data, err = clientObj.read()
		}
		break
	case "write":
		clientObj.prepWriteReq(rArr)
		data, err = clientObj.write(false)
		break
	case "read":
		if !isRangeRequest(clientObj.requestKey) {
			data, err = clientObj.read()
		} else {
			data, err = clientObj.rangeRead()
		}
		break
	default:
		log.Error("Invalid operation type")
	}
	return data, err
}

func (clientObj *clientHandler) processConfig() ([]byte, error) {
	return clientObj.clientAPIObj.GetPMDBServerConfig()
}

func (clientObj *clientHandler) processMembership() ([]byte, error){
	toJson := clientObj.clientAPIObj.GetMembership()
	return json.MarshalIndent(toJson, "", " ")
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

func (clientObj *clientHandler) processGossip() ([]byte, error) {
	fileData, err := clientObj.clientAPIObj.GetPMDBServerConfig()
	if err != nil {
		log.Error("Error while getting pmdb server config data : ", err)
		return nil, err
	}
	f, _ := os.OpenFile(clientObj.resultFile+".json", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	f.WriteString(string(fileData))

	return fileData, err
}

func (clientObj *clientHandler) processProxyStat() ([]byte, error) {
	clientObj.clientAPIObj.ServerChooseAlgorithm = 2
	clientObj.clientAPIObj.UseSpecificServerName = clientObj.requestKey
	resBytes, err := clientObj.clientAPIObj.Request(nil, "/stat", false)
	if err != nil {
		log.Error("Error while sending request to proxy : ", err)
	}
	return resBytes, err
}

func (clientObj *clientHandler) processLookoutInfo() ([]byte, error) {
	clientObj.clientAPIObj.ServerChooseAlgorithm = 2
	clientObj.clientAPIObj.UseSpecificServerName = clientObj.rncui

	var b bytes.Buffer
	var r []byte

	err := clientObj.prepareLOInfoRequest(&b)
	if err != nil {
		log.Error("Error while preparing lookout request")
		return nil, err
	}

	r, err = clientObj.clientAPIObj.Request(b.Bytes(), "/v1/", false)
	if err != nil {
		log.Error("Error while sending request : ", err)
		return nil, err
	}

	return r, err
}

func (clientObj *clientHandler) waitServiceInit(service string) error {
	err := clientObj.clientAPIObj.TillReady(service, clientObj.serviceRetry)
	if err != nil {
		opStat := prepareOutput(-1, "setup", "", err.Error(), 0)
		clientObj.writeData2Json(opStat)
		log.Error(err)
	}
	return err
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

//Write to Json
func (cli *clientHandler) writeData2Json(data interface{}) {
	file, err := json.MarshalIndent(data, "", " ")
	err = ioutil.WriteFile(cli.resultFile+".json", file, 0644)
	if err != nil {
		log.Error("Error in writing output to the file : ", err)
	}
}

func (clientObj *clientHandler) performLeaseReq(resource, client string) ([]byte, error) {
	clientObj.clientAPIObj.TillReady("PROXY", clientObj.serviceRetry)

	op := getLeaseOperationType(clientObj.operation)

	var lrh leaseClientLib.LeaseClientReqHandler
	err := clientObj.prepareLeaseHandlers(&lrh)
	if err != nil {
		log.Error("Error while preparing lease handlers : ", err)
		return nil, err
	}
	err = lrh.InitLeaseReq(client, resource, op)
	if err != nil {
		log.Error("error while initializing lease req : ", err)
		return nil, err
	}
	err = lrh.LeaseOperationOverHTTP()
	if err != nil {
		log.Error("Error sending lease request : ", err)
		return nil, err
	}

	data, err := json.MarshalIndent(lrh, "", " ")

	return data, err
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
			clientObj.writeData2Json(opStat)
			log.Error(err)
			os.Exit(1)
		}
	}()

	//Wait till client API Object is ready
	clientObj.waitServiceInit("")

	var passNext bool
	var rdata []byte

	switch clientObj.operation {
	case "rw":
		fallthrough
	case "write":
		fallthrough
	case "read":
		rArr := clientObj.getKVArray()
		rdata, err = clientObj.processReadWriteReq(rArr)
		break
	case "config":
		rdata, err = clientObj.processConfig()
		break

	case "membership":
		rdata, err = clientObj.processMembership()
		break

	case "general":
		clientObj.processGeneral()
		rdata = nil
		break

	case "nisd":
		clientObj.processNisd()
		rdata = nil
		break

	case "Gossip":
		passNext = true
		rdata = nil
		break

	case "NISDGossip":
		nisdDataMap := clientObj.getNISDInfo()
		rdata, _ = json.MarshalIndent(nisdDataMap, "", " ")
		if !passNext {
			break
		}
		fallthrough

	case "PMDBGossip":
		rdata, err = clientObj.processGossip()
		break

	case "ProxyStat":
		rdata, err = clientObj.processProxyStat()
		break

	case "LookoutInfo":
		rdata, err = clientObj.processLookoutInfo()
		break

	//Lease Operations
	case "GetLease":
		fallthrough
	case "LookupLease":
		fallthrough
	case "RefreshLease":
		rdata, err = clientObj.performLeaseReq(clientObj.requestKey, clientObj.requestValue)
		break
	}

	if rdata != nil {
		err = clientObj.complete(rdata)
		if err != nil {
			log.Error("Failed to write the response to the file")
		}
	}
}
