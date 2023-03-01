package main

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"sync/atomic"

	leaseLib "common/leaseLib"
	pmdbClient "niova/go-pumicedb-lib/client"
	PumiceDBCommon "niova/go-pumicedb-lib/common"

	log "github.com/sirupsen/logrus"

	uuid "github.com/satori/go.uuid"
)

type state int

const (
	ACQUIRED      state = 0
	FREE                = 1
	TRANSITIONING       = 2
)

var (
	operationsMap = map[string]int{
		"GET":     leaseLib.GET,
		"PUT":     leaseLib.PUT,
		"LOOKUP":  leaseLib.LOOKUP,
		"REFRESH": leaseLib.REFRESH,
	}
	kvMap = make(map[uuid.UUID]uuid.UUID)
	rdMap = make(map[uuid.UUID]uuid.UUID)
)

type leaseHandler struct {
	raftUUID      uuid.UUID
	pmdbClientObj *pmdbClient.PmdbClientObj
	jsonFilePath  string
	logFilePath   string
	numOfLeases   int
	readJsonFile  string
}

// will be used to write to json file
type JsonLeaseReq struct {
	Client    uuid.UUID
	Resource  uuid.UUID
	Operation string
}

type JsonLeaseResp struct {
	Client     uuid.UUID
	Resource   uuid.UUID
	Status     string
	LeaseState string
	TTL        int
	TimeStamp  leaseLib.LeaderTS
}

// will be used to write req and res to json file
type writeObj struct {
	Request  JsonLeaseReq
	Response JsonLeaseResp
}

func usage() {
	flag.PrintDefaults()
	os.Exit(0)
}

func parseOperation(str string) (int, bool) {
	op, ok := operationsMap[str]
	return op, ok
}

func (handler *leaseHandler) getRNCUI() string {
	idq := atomic.AddUint64(&handler.pmdbClientObj.WriteSeqNo, uint64(1))
	rncui := fmt.Sprintf("%s:0:0:0:%d", handler.pmdbClientObj.AppUUID, idq)
	return rncui
}

func getStringOperation(op int) string {
	switch op {
	case leaseLib.GET:
		return "GET"
	case leaseLib.PUT:
		return "PUT"
	case leaseLib.LOOKUP:
		return "LOOKUP"
	case leaseLib.REFRESH:
		return "REFRESH"
	}
	return "UNKNOWN"
}

func getStringLeaseState(leaseState int) string {
	switch leaseState {
	case leaseLib.GRANTED:
		return "GRANTED"
	case leaseLib.INPROGRESS:
		return "IN-PROGRESS"
	case leaseLib.EXPIRED:
		return "EXPIRED"
	case leaseLib.AIU:
		return "ALREADY-IN-USE"
	case leaseLib.INVALID:
		return "INVALID"
	}
	return "UNKNOWN"
}

func prepareJsonResponse(requestObj leaseLib.LeaseReq, responseObj leaseLib.LeaseStruct) writeObj {
	req := JsonLeaseReq{
		Client:    requestObj.Client,
		Resource:  requestObj.Resource,
		Operation: getStringOperation(requestObj.Operation),
	}
	resp := JsonLeaseResp{
		Client:     responseObj.Client,
		Resource:   responseObj.Resource,
		Status:     responseObj.Status,
		LeaseState: getStringLeaseState(responseObj.LeaseState),
		TTL:        responseObj.TTL,
		TimeStamp:  responseObj.TimeStamp,
	}
	res := writeObj{
		Request:  req,
		Response: resp,
	}
	return res
}

/*
Structure : leaseHandler
Method	  : getCmdParams
Arguments : None
Return(s) : None

Description : Parse command line params and load into leaseHandler sturct
*/
func (handler *leaseHandler) getCmdParams() leaseLib.LeaseReq {
	var stringOperation, strClientUUID, strResourceUUID, strRaftUUID string
	var requestObj leaseLib.LeaseReq
	var tempOperation int
	var ok bool
	var err error

	flag.StringVar(&strClientUUID, "u", uuid.NewV4().String(), "ClientUUID - UUID of the requesting client")
	flag.StringVar(&strResourceUUID, "v", uuid.NewV4().String(), "ResourceUUID - UUID of the requested resource")
	flag.StringVar(&strRaftUUID, "ru", "NULL", "RaftUUID - UUID of the raft cluster")
	flag.StringVar(&handler.jsonFilePath, "j", "/tmp", "Output file path")
	flag.StringVar(&handler.logFilePath, "l", "", "Log file path")
	flag.StringVar(&stringOperation, "o", "LOOKUP", "Operation - GET/PUT/LOOKUP/REFRESH")
	flag.IntVar(&handler.numOfLeases, "n", 1, "Pass number of leases(Default 1)")
	flag.StringVar(&handler.readJsonFile, "f", "", "Read JSON file")

	flag.Usage = usage
	flag.Parse()
	if flag.NFlag() == 0 {
		usage()
		os.Exit(-1)
	}
	tempOperation, ok = parseOperation(stringOperation)
	if !ok {
		usage()
		os.Exit(-1)
	}
	requestObj.Operation = int(tempOperation)
	handler.raftUUID, err = uuid.FromString(strRaftUUID)
	if err != nil {
		usage()
		os.Exit(-1)
	}
	requestObj.Client, err = uuid.FromString(strClientUUID)
	if err != nil {
		usage()
		os.Exit(-1)
	}
	requestObj.Resource, err = uuid.FromString(strResourceUUID)
	if err != nil {
		usage()
		os.Exit(-1)
	}
	return requestObj
}

/*
Structure : leaseHandler
Method	  : startPMDBClient
Arguments : None
Return(s) : error

Description : Start PMDB Client object for ClientUUID and RaftUUID
*/
func (handler *leaseHandler) startPMDBClient(client string) error {
	var err error

	//Get clientObj
	log.Info("Raft UUID - ", handler.raftUUID.String(), " Client UUID - ", client)
	handler.pmdbClientObj = pmdbClient.PmdbClientNew(handler.raftUUID.String(), client)
	if handler.pmdbClientObj == nil {
		return errors.New("PMDB Client Obj could not be initialized")
	}

	//Start PMDB Client
	err = handler.pmdbClientObj.Start()
	if err != nil {
		return err
	}

	leaderUuid, err := handler.pmdbClientObj.PmdbGetLeader()
	for err != nil {
		leaderUuid, err = handler.pmdbClientObj.PmdbGetLeader()
	}
	log.Info("Leader uuid : ", leaderUuid.String())

	//Store encui in AppUUID
	handler.pmdbClientObj.AppUUID = uuid.NewV4().String()
	return nil
}

/*
Description : Generate N number of client and resource uuids
*/

func generateUuids(numOfLeases int64) map[uuid.UUID]uuid.UUID {

	noUUID := numOfLeases

	for i := int64(0); i < noUUID; i++ {
		clientUUID := uuid.NewV4()
		resourceUUID := uuid.NewV4()
		kvMap[clientUUID] = resourceUUID

	}

	return kvMap
}

/*
Description : Read JSON outfile and parse it.
*/

func readJsonFile(filename string) map[uuid.UUID]uuid.UUID {

	// Open our jsonFile
	jsonFile, err := os.Open(filename + ".json")
	// if we os.Open returns an error then handle it
	if err != nil {
		fmt.Println(err)
	}

	// defer the closing of our jsonFile so that we can parse it later on
	defer jsonFile.Close()

	// read our opened xmlFile as a byte array.
	byteValue, _ := ioutil.ReadAll(jsonFile)
	// we initialize our Users array
	var writeObjArr []writeObj

	// we unmarshal our byteArray which contains our
	// jsonFile's content into 'writeObjArr' which we defined above
	json.Unmarshal(byteValue, &writeObjArr)

	// we iterate through every user within our writeObjArr array and
	// print out the user Type, their name, and their facebook url
	// as just an example
	for i := range writeObjArr {
		rdMap[writeObjArr[i].Request.Client] = writeObjArr[i].Request.Resource
	}

	return rdMap
}

/*
Structure : leaseHandler
Method	  : Write()
Arguments : LeaseReq, rncui, *LeaseResp
Return(s) : error

Description : Wrapper function for WriteEncoded() function
*/

func (handler *leaseHandler) Write(requestObj leaseLib.LeaseReq, rncui string, response *[]byte) error {
	var err error
	var requestBytes bytes.Buffer
	var replySize int64

	enc := gob.NewEncoder(&requestBytes)
	err = enc.Encode(requestObj)
	if err != nil {
		return err
	}
	reqArgs := &pmdbClient.PmdbReqArgs{
		Rncui:       rncui,
                ReqByteArr:  requestBytes.Bytes(),
                GetResponse: 1,
                ReplySize:   &replySize,
                Response:    response,
                ReqType:     1,
	}

	err = handler.pmdbClientObj.WriteEncodedAndGetResponse(reqArgs)

	return err
}

/*
Structure : leaseHandler
Method	  : Read()
Arguments : LeaseReq, rncui, *response
Return(s) : error

Description : Wrapper function for ReadEncoded() function
*/
func (handler *leaseHandler) Read(requestObj leaseLib.LeaseReq, rncui string, response *[]byte) error {
	var err error
	var requestBytes bytes.Buffer
	enc := gob.NewEncoder(&requestBytes)
	err = enc.Encode(requestObj)
	if err != nil {
		return err
	}
	reqArgs := &pmdbClient.PmdbReqArgs{
		Rncui:      "",
		ReqByteArr: requestBytes.Bytes(),
		Response:   response,
		ReqType:    1,

	}

	return handler.pmdbClientObj.ReadEncoded(reqArgs)
}

/*
Structure : leaseHandler
Method	  : get()
Arguments : leaseLib.LeaseReq
Return(s) : error

Description : Handler function for get() operation
              Acquire a lease on a particular resource
*/
func (handler *leaseHandler) get(requestObj leaseLib.LeaseReq) (leaseLib.LeaseStruct, error) {
	var err error
	var responseBytes []byte
	var responseObj leaseLib.LeaseStruct

	rncui := handler.getRNCUI()

	err = handler.Write(requestObj, rncui, &responseBytes)
	if err != nil {
		return responseObj, err
	}

	dec := gob.NewDecoder(bytes.NewBuffer(responseBytes))
	err = dec.Decode(&responseObj)
	if err != nil {
		return responseObj, err
	}

	log.Info("Write request status - ", responseObj.Status)

	return responseObj, err
}

/*
Structure : leaseHandler
Method	  : lookup()
Arguments : leaseLib.LeaseReq
Return(s) : error

Description : Handler function for lookup() operation
              Lookup lease info of a particular resource
*/
func (handler *leaseHandler) lookup(requestObj leaseLib.LeaseReq) (leaseLib.LeaseStruct, error) {
	var err error
	var responseBytes []byte
	var responseObj leaseLib.LeaseStruct

	err = handler.Read(requestObj, "", &responseBytes)
	if err != nil {
		return responseObj, err
	}

	dec := gob.NewDecoder(bytes.NewBuffer(responseBytes))
	err = dec.Decode(&responseObj)
	if err != nil {
		return responseObj, err
	}

	return responseObj, err
}

/*
Structure : leaseHandler
Method	  : refresh()
Arguments : leaseLib.LeaseReq
Return(s) : error

Description : Handler function for refresh() operation
              Refresh lease of a owned resource
*/
func (handler *leaseHandler) refresh(requestObj leaseLib.LeaseReq) (leaseLib.LeaseStruct, error) {
	var err error
	var responseBytes []byte
	var responseObj leaseLib.LeaseStruct

	rncui := handler.getRNCUI()
	err = handler.Write(requestObj, rncui, &responseBytes)
	if err != nil {
		return responseObj, err
	}

	dec := gob.NewDecoder(bytes.NewBuffer(responseBytes))
	err = dec.Decode(&responseObj)
	if err != nil {
		return responseObj, err
	}

	log.Info("Refresh request status - ", responseObj.Status)

	return responseObj, err
}

/*
Structure : leaseHandler
Method    : multiGet()
Arguments : leaseLib.LeaseReq

Description: Perform Multiple GET lease operation
*/

func (handler *leaseHandler) multiGet(requestObj leaseLib.LeaseReq) []writeObj {

	var err error
	var res writeObj
	var responseObj leaseLib.LeaseStruct
	var responseObjArr []writeObj

	kvMap = generateUuids(int64(handler.numOfLeases))
	for key, value := range kvMap {
		requestObj.Client = key
		requestObj.Resource = value
		// get lease for multiple clients and resources
		responseObj, err = handler.get(requestObj)
		if err != nil {
			log.Error(err)
			responseObj.Status = err.Error()
		} else {
			responseObj.Status = "Success"
		}
		res = prepareJsonResponse(requestObj, responseObj)
		responseObjArr = append(responseObjArr, res)
	}

	return responseObjArr
}

/*
Structure : leaseHandler
Method    : multiLookup()
Arguments : leaseLib.LeaseReq

Description: Perform Multiple LOOKUP lease operation
*/

func (handler *leaseHandler) multiLookup(requestObj leaseLib.LeaseReq) []writeObj {

	var err error
	var res writeObj
	var responseObj leaseLib.LeaseStruct
	var responseObjArr []writeObj
	rdMap = readJsonFile(handler.readJsonFile)
	for key, value := range rdMap {
		requestObj.Client = key
		requestObj.Resource = value
		// lookup lease for multiple clients and resources
		responseObj, err = handler.lookup(requestObj)
		if err != nil {
			log.Error(err)
			responseObj.Status = err.Error()
		} else {
			responseObj.Status = "Success"
		}
		res = prepareJsonResponse(requestObj, responseObj)
		responseObjArr = append(responseObjArr, res)
	}

	return responseObjArr
}

/*
Structure : leaseHandler
Method	  : writeToJson
Arguments : struct
Return(s) : error

Description : Write response/error to json file
*/
func (handler *leaseHandler) writeToJson(toJson interface{}, jsonFilePath string) {
	file, err := json.MarshalIndent(toJson, "", " ")
	err = ioutil.WriteFile(jsonFilePath+".json", file, 0644)
	if err != nil {
		log.Error("Error writing to outfile : ", err)
	}
}

func main() {
	leaseObjHandler := leaseHandler{}

	// Load cmd params
	requestObj := leaseObjHandler.getCmdParams()

	/*
		Initialize Logging
	*/
	err := PumiceDBCommon.InitLogger(leaseObjHandler.logFilePath)
	if err != nil {
		log.Error("Error while initializing the logger ", err)
	}

	err = leaseObjHandler.startPMDBClient(requestObj.Client.String())
	if err != nil {
		log.Error(err)
		os.Exit(-1)
	}

	var responseObj leaseLib.LeaseStruct

	var responseObjArr []writeObj
	switch requestObj.Operation {
	case leaseLib.GET:
		if leaseObjHandler.numOfLeases >= 1 {
			responseObjArr = leaseObjHandler.multiGet(requestObj)
			leaseObjHandler.writeToJson(responseObjArr, leaseObjHandler.jsonFilePath)
		} else {

			// get lease
			responseObj, err = leaseObjHandler.get(requestObj)
			if err != nil {
				log.Error(err)
				responseObj.Status = err.Error()
			} else {
				responseObj.Status = "Success"
			}
			res := prepareJsonResponse(requestObj, responseObj)
			leaseObjHandler.writeToJson(res, leaseObjHandler.jsonFilePath)
		}

	case leaseLib.LOOKUP:
		if leaseObjHandler.numOfLeases >= 1 {
			responseObjArr = leaseObjHandler.multiLookup(requestObj)
			leaseObjHandler.writeToJson(responseObjArr, leaseObjHandler.jsonFilePath)
		} else {

			// lookup lease
			responseObj, err = leaseObjHandler.lookup(requestObj)
			if err != nil {
				log.Error(err)
				responseObj.Status = err.Error()
			} else {
				responseObj.Status = "Success"
			}
			res := prepareJsonResponse(requestObj, responseObj)
			leaseObjHandler.writeToJson(res, leaseObjHandler.jsonFilePath)
		}
	case leaseLib.REFRESH:
		// refresh lease
		responseObj, err = leaseObjHandler.refresh(requestObj)
		if err != nil {
			log.Error(err)
			responseObj.Status = err.Error()
		} else {
			responseObj.Status = "Success"
		}
		res := prepareJsonResponse(requestObj, responseObj)
		leaseObjHandler.writeToJson(res, leaseObjHandler.jsonFilePath)
	}

	log.Info("-----END OF EXECUTION-----")
}
