package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"

	leaseClientLib "LeaseLib/leaseClient"
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
		"GET":             leaseLib.GET,
		"PUT":             leaseLib.PUT,
		"LOOKUP":          leaseLib.LOOKUP,
		"REFRESH":         leaseLib.REFRESH,
		"GET_VALIDATE":    leaseLib.GET_VALIDATE,
		"LOOKUP_VALIDATE": leaseLib.LOOKUP_VALIDATE,
	}
	kvMap = make(map[uuid.UUID]uuid.UUID)
	rdMap = make(map[uuid.UUID]uuid.UUID)
)

type leaseHandler struct {
	clientObj    leaseClientLib.LeaseClient
	jsonFilePath string
	logFilePath  string
	numOfLeases  int
	readJsonFile string
}

type multiLease struct {
	Request  map[uuid.UUID]uuid.UUID
	Response map[string]interface{}
}

func usage() {
	flag.PrintDefaults()
	os.Exit(0)
}

func parseOperation(str string) (int, bool) {
	op, ok := operationsMap[str]
	return op, ok
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

type WriteObj struct {
	Request  JsonLeaseReq
	Response JsonLeaseResp
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

func getStringOperation(op int) string {
	switch op {
	case leaseLib.GET:
		return "GET"
	case leaseLib.GET_VALIDATE:
		return "GET_VALIDATE"
	case leaseLib.PUT:
		return "PUT"
	case leaseLib.LOOKUP:
		return "LOOKUP"
	case leaseLib.LOOKUP_VALIDATE:
		return "LOOKUP_VALIDATE"
	case leaseLib.REFRESH:
		return "REFRESH"
	}
	return "UNKNOWN"
}

func prepareLeaseJsonResponse(requestObj leaseLib.LeaseReq, responseObj leaseLib.LeaseRes) WriteObj {
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
	res := WriteObj{
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
//TODO make it a method for leaseHandler
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
	flag.StringVar(&stringOperation, "o", "LOOKUP", "Operation - GET/PUT/LOOKUP/REFRESH/GET_VALIDATE")
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
	handler.clientObj.RaftUUID, err = uuid.FromString(strRaftUUID)
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
	log.Info("Raft UUID - ", handler.clientObj.RaftUUID.String(), " Client UUID - ", client)
	handler.clientObj.PmdbClientObj = pmdbClient.PmdbClientNew(handler.clientObj.RaftUUID.String(), client)
	if handler.clientObj.PmdbClientObj == nil {
		return errors.New("PMDB Client Obj could not be initialized")
	}

	//Start PMDB Client
	err = handler.clientObj.PmdbClientObj.Start()
	if err != nil {
		return err
	}

	leaderUuid, err := handler.clientObj.PmdbClientObj.PmdbGetLeader()
	for err != nil {
		leaderUuid, err = handler.clientObj.PmdbClientObj.PmdbGetLeader()
	}
	log.Info("Leader uuid : ", leaderUuid.String())

	//Store encui in AppUUID
	handler.clientObj.PmdbClientObj.AppUUID = uuid.NewV4().String()
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

	var res multiLease

	json.Unmarshal(byteValue, &res)

	for key, value := range res.Request {
		rdMap[key] = value
	}

	return rdMap
}

/*
Description: Perform GET lease operation
*/

func performGet(requestObj leaseLib.LeaseReq, handler *leaseHandler, reqHandler *leaseClientLib.LeaseReqHandler) []WriteObj {

	var err error
	var res WriteObj
	var responseObj leaseLib.LeaseRes
	var responseObjArr []WriteObj

	//If user passed UUID through cmdline
	if requestObj.Client != uuid.Nil && requestObj.Resource != uuid.Nil {
		handler.numOfLeases = 1
		kvMap[requestObj.Client] = requestObj.Resource
	} else {
		handler.numOfLeases >= 1
		kvMap = generateUuids(int64(handler.numOfLeases))
	}

	for key, value := range kvMap {
		requestObj.Client = key
		requestObj.Resource = value
		// Perform get lease
		err = reqHandler.Get()
		if err != nil {
			log.Error(err)
			responseObj.Status = err.Error()
		} else {
			responseObj.Status = "Success"
		}
		res = prepareLeaseJsonResponse(requestObj, responseObj)
		responseObjArr = append(responseObjArr, res)
	}

	return responseObjArr
}

/*
Description: Perform LOOKUP lease operation
*/

func performLookup(requestObj leaseLib.LeaseReq, handler *leaseHandler, reqHandler *leaseClientLib.LeaseReqHandler) []WriteObj {

	var err error
	var res WriteObj
	var responseObj leaseLib.LeaseRes
	var responseObjArr []WriteObj

	//If user passed UUID through cmdline
	if requestObj.Client != uuid.Nil && requestObj.Resource != uuid.Nil {
		handler.numOfLeases = 1
		rdMap[requestObj.Client] = requestObj.Resource
	} else {
		rdMap = readJsonFile(handler.readJsonFile)
	}

	for key, value := range rdMap {
		reqHandler.LeaseReq.Client = key
		reqHandler.LeaseReq.Resource = value
		// Perform lookup lease
		err = reqHandler.Lookup()
		if err != nil {
			log.Error(err)
			responseObj.Status = err.Error()
		} else {
			responseObj.Status = "Success"
		}
		res = prepareLeaseJsonResponse(reqHandler.LeaseReq, reqHandler.LeaseRes)
		responseObjArr = append(responseObjArr, res)
	}

	return responseObjArr
}

/*
description: It compare the multiple get leases response.
	     and dump it json file.
*/

func compareGetResponse(requestObj leaseLib.LeaseReq, handler *leaseHandler, reqHandler *leaseClientLib.LeaseReqHandler) {

	var responseObjArr []WriteObj
	var res multiLease
	uuidMap := make(map[uuid.UUID]uuid.UUID)
	mapString := make(map[string]interface{})
	responseObjArr = performGet(requestObj, handler, reqHandler)

	//Fill the map with clients and resources
	for i := range responseObjArr {
		uuidMap[responseObjArr[i].Request.Client] = responseObjArr[i].Request.Resource
	}

	if handler.numOfLeases == 1 {
		res = fillGetValidateResponse(requestObj, handler, reqHandler)
	} else if handler.numOfLeases > 1 {
		//Check if prev element have same LeaseState and LeaderTeerm as current response.
		for i := 0; i < len(responseObjArr)-1; i++ {
			if responseObjArr[i].Response.TimeStamp.LeaderTerm == responseObjArr[i+1].Response.TimeStamp.LeaderTerm {
				mapString["LeaderTerm"] = responseObjArr[i+1].Response.TimeStamp.LeaderTerm
			}

			if responseObjArr[i].Response.LeaseState == responseObjArr[i+1].Response.LeaseState {
				mapString["LeaseState"] = responseObjArr[i+1].Response.LeaseState
			} else {
				fmt.Println("LeaseState not matched")
			}
		}
	}
	//Fill the structure
	res = multiLease{
		Request:  uuidMap,
		Response: mapString,
	}

	writeToJson(res, handler.jsonFilePath)
}

/*
Description: It compare the multiple lookup leases response
	     and dump to json file.
*/

func compareLoookupResponse(requestObj leaseLib.LeaseReq, handler *leaseHandler, reqHandler *leaseClientLib.LeaseReqHandler) {

	var responseObjArr []WriteObj
	toJson := make(map[string]interface{})
	responseObjArr = performLookup(requestObj, handler, reqHandler)

	if handler.numOfLeases == 1 {
		toJson = fillLookupValidateResponse(requestObj, handler, reqHandler)
	} else if handler.numOfLeases > 1 {
		for i := 0; i < len(responseObjArr)-1; i++ {
			if responseObjArr[i].Response.TimeStamp.LeaderTerm == responseObjArr[i+1].Response.TimeStamp.LeaderTerm {
				toJson["LeaderTerm"] = responseObjArr[i+1].Response.TimeStamp.LeaderTerm
			}
			if responseObjArr[i].Response.LeaseState == responseObjArr[i+1].Response.LeaseState {
				toJson["LeaseState"] = responseObjArr[i+1].Response.LeaseState
			} else {
				fmt.Println("LeaseState not matched")
			}
		}
	}

	writeToJson(toJson, handler.jsonFilePath)
}

/*
Description: Fill the response of Lookup validation
*/

func fillLookupValidateResponse(requestObj leaseLib.LeaseReq, handler *leaseHandler, reqHandler *leaseClientLib.LeaseReqHandler) map[string]interface{} {

	var responseObjArr []WriteObj
	toJson := make(map[string]interface{})
	responseObjArr = performLookup(requestObj, handler, reqHandler)

	if len(responseObjArr) == 1 {
		toJson["Client"] = responseObjArr[0].Response.Client
		toJson["Resource"] = responseObjArr[0].Response.Resource
		toJson["Status"] = responseObjArr[0].Response.Status
		toJson["LeaderTerm"] = responseObjArr[0].Response.TimeStamp.LeaderTerm
		toJson["LeaderTime"] = responseObjArr[0].Response.TimeStamp.LeaderTime
		toJson["LeaseState"] = responseObjArr[0].Response.LeaseState
		toJson["TTL"] = responseObjArr[0].Response.TTL
	}

	return toJson
}

/*
Description: Fill the response of Get validation
*/

func fillGetValidateResponse(requestObj leaseLib.LeaseReq, handler *leaseHandler, reqHandler *leaseClientLib.LeaseReqHandler) multiLease {

	var responseObjArr []WriteObj
	uuidMap := make(map[uuid.UUID]uuid.UUID)
	mapString := make(map[string]interface{})
	responseObjArr = performGet(requestObj, handler, reqHandler)

	//Fill the map with clients and resources
	for i := range responseObjArr {
		uuidMap[responseObjArr[i].Request.Client] = responseObjArr[i].Request.Resource
	}

	if len(responseObjArr) == 1 {
		mapString["Client"] = responseObjArr[0].Response.Client
		mapString["Resource"] = responseObjArr[0].Response.Resource
		mapString["Status"] = responseObjArr[0].Response.Status
		mapString["LeaderTerm"] = responseObjArr[0].Response.TimeStamp.LeaderTerm
		mapString["LeaderTime"] = responseObjArr[0].Response.TimeStamp.LeaderTime
		mapString["LeaseState"] = responseObjArr[0].Response.LeaseState
		mapString["TTL"] = responseObjArr[0].Response.TTL
	}

	//Fill the structure
	getValidateRes := multiLease{
		Request:  uuidMap,
		Response: mapString,
	}

	return getValidateRes
}

/*
Structure : leaseHandler
Method	  : writeToJson
Arguments : struct
Return(s) : error

Description : Write response/error to json file
*/
func writeToJson(toJson interface{}, jsonFilePath string) {
	file, err := json.MarshalIndent(toJson, "", " ")
	err = ioutil.WriteFile(jsonFilePath+".json", file, 0644)
	if err != nil {
		log.Error("Error writing to outfile : ", err)
	}
}

func main() {
	leaseObjHandler := leaseHandler{}
	leaseReqHandler := leaseClientLib.LeaseReqHandler{}

	// Load cmd params
	leaseReqHandler.LeaseReq = leaseObjHandler.getCmdParams()

	/*
		Initialize Logging
	*/
	err := PumiceDBCommon.InitLogger(leaseObjHandler.logFilePath)
	if err != nil {
		log.Error("Error while initializing the logger ", err)
	}

	err = leaseObjHandler.startPMDBClient(leaseReqHandler.LeaseReq.Client.String())
	if err != nil {
		log.Error(err)
		os.Exit(-1)
	}

	leaseReqHandler.LeaseClientObj = leaseObjHandler.clientObj
	var responseObj leaseLib.LeaseRes

	switch leaseReqHandler.LeaseReq.Operation {
	case leaseLib.GET:
		// get lease
		//TODO Better error handling
		err = leaseReqHandler.Get()
		if err != nil {
			log.Error(err)
			responseObj.Status = err.Error()
		} else {
			responseObj.Status = "Success"
		}
		res := prepareLeaseJsonResponse(leaseReqHandler.LeaseReq, leaseReqHandler.LeaseRes)
		writeToJson(res, leaseObjHandler.jsonFilePath)

	case leaseLib.LOOKUP:
		// lookup lease
		//TODO Better error handling
		err = leaseReqHandler.Lookup()
		if err != nil {
			log.Error(err)
			responseObj.Status = err.Error()
		} else {
			responseObj.Status = "Success"
		}
		res := prepareLeaseJsonResponse(leaseReqHandler.LeaseReq, leaseReqHandler.LeaseRes)
		writeToJson(res, leaseObjHandler.jsonFilePath)

	case leaseLib.REFRESH:
		// refresh lease
		//TODO Better error handling
		err = leaseReqHandler.Refresh()
		if err != nil {
			log.Error(err)
			responseObj.Status = err.Error()
		} else {
			responseObj.Status = "Success"
		}
		res := prepareLeaseJsonResponse(leaseReqHandler.LeaseReq, leaseReqHandler.LeaseRes)
		writeToJson(res, leaseObjHandler.jsonFilePath)

	case leaseLib.GET_VALIDATE:
		//get and validate lease
		compareGetResponse(leaseReqHandler.LeaseReq, &leaseObjHandler, &leaseReqHandler)

	case leaseLib.LOOKUP_VALIDATE:
		// lookup and validate lease
		compareLoookupResponse(leaseReqHandler.LeaseReq, &leaseObjHandler, &leaseReqHandler)
	}

	log.Info("-----END OF EXECUTION-----")
}
