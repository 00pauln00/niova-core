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
	case leaseLib.PUT:
		return "PUT"
	case leaseLib.LOOKUP:
		return "LOOKUP"
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
Structure : leaseHandler
Method    : multiGet()
Arguments : leaseLib.LeaseReq

Description: Perform Multiple GET lease operation
*/

func multiGet(requestObj leaseLib.LeaseReq, handler *leaseHandler) []WriteObj {

	var err error
	var res WriteObj
	var responseObj leaseLib.LeaseRes
	var responseObjArr []WriteObj

	kvMap = generateUuids(int64(handler.numOfLeases))
	for key, value := range kvMap {
		requestObj.Client = key
		requestObj.Resource = value
		// get lease for multiple clients and resources
		responseObj, err = handler.clientObj.Get(requestObj)
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
Structure : leaseHandler
Method    : multiLookup()
Arguments : leaseLib.LeaseReq

Description: Perform Multiple LOOKUP lease operation
*/

func multiLookup(requestObj leaseLib.LeaseReq, handler *leaseHandler) []WriteObj {

	var err error
	var res WriteObj
	var responseObj leaseLib.LeaseRes
	var responseObjArr []WriteObj
	rdMap = readJsonFile(handler.readJsonFile)
	for key, value := range rdMap {
		requestObj.Client = key
		requestObj.Resource = value
		// lookup lease for multiple clients and resources
		responseObj, err = handler.clientObj.Lookup(requestObj)
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
structure : leasehandler
method    : getvalidate()
arguments : leaselib.leasereq

description: It validates the multiple get leases response.
	     and dump it json file.
*/

func getValidate(requestObj leaseLib.LeaseReq, handler *leaseHandler) {

	var responseObjArr []WriteObj
	uuidMap := make(map[uuid.UUID]uuid.UUID)
	mapString := make(map[string]interface{})
	responseObjArr = multiGet(requestObj, handler)

	//Fill the map with clients and resources
	for i := range responseObjArr {
		uuidMap[responseObjArr[i].Request.Client] = responseObjArr[i].Request.Resource
	}

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

	//Fill the structure
	res := multiLease{
		Request:  uuidMap,
		Response: mapString,
	}

	writeToJson(res, handler.jsonFilePath)
}

/*
Structure : leaseHandler
Method    : multiLookupValidate()
Arguments : leaseLib.LeaseReq

Description: It validates the multiple lookup leases response
	     and dump to json file.
*/

func multiLookupValidate(requestObj leaseLib.LeaseReq, handler *leaseHandler) {

	var responseObjArr []WriteObj
	toJson := make(map[string]interface{})
	responseObjArr = multiLookup(requestObj, handler)

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
	writeToJson(toJson, handler.jsonFilePath)
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

/*
Structure : leaseHandler
Method    : lookup_and_validate()
Arguments : leaseLib.LeaseReq
Return(s) : error

Description : Handler function for lookup() operation
              Lookup lease info of a particular resource
              and validate that lease is valid.
*/
func lookup_validate(requestObj leaseLib.LeaseReq, handler *leaseHandler) (leaseLib.LeaseRes, error) {
	var err error
	var responseBytes []byte
	var responseObj leaseLib.LeaseRes

	err = handler.clientObj.Read(requestObj, "", &responseBytes)
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

	var responseObj leaseLib.LeaseRes

	switch requestObj.Operation {
	case leaseLib.GET:
		// get lease
		responseObj, err = leaseObjHandler.clientObj.Get(requestObj)
		if err != nil {
			log.Error(err)
			responseObj.Status = err.Error()
		} else {
			responseObj.Status = "Success"
		}
		res := prepareLeaseJsonResponse(requestObj, responseObj)
		writeToJson(res, leaseObjHandler.jsonFilePath)

	case leaseLib.LOOKUP:
		// lookup lease
		responseObj, err = leaseObjHandler.clientObj.Lookup(requestObj)
		if err != nil {
			log.Error(err)
			responseObj.Status = err.Error()
		} else {
			responseObj.Status = "Success"
		}
		res := prepareLeaseJsonResponse(requestObj, responseObj)
		writeToJson(res, leaseObjHandler.jsonFilePath)

	case leaseLib.REFRESH:
		// refresh lease
		responseObj, err = leaseObjHandler.clientObj.Refresh(requestObj)
		if err != nil {
			log.Error(err)
			responseObj.Status = err.Error()
		} else {
			responseObj.Status = "Success"
		}
		res := prepareLeaseJsonResponse(requestObj, responseObj)
		writeToJson(res, leaseObjHandler.jsonFilePath)

	case leaseLib.GET_VALIDATE:
		//Perform and validate multiple get leases
		if leaseObjHandler.numOfLeases >= 1 {
			getValidate(requestObj, &leaseObjHandler)
		}

	case leaseLib.LOOKUP_VALIDATE:
		//Perform and validate multiple lookup leases
		if leaseObjHandler.numOfLeases >= 1 {
			multiLookupValidate(requestObj, &leaseObjHandler)
		} else {

			// lookup and validate lease
			responseObj, err = lookup_validate(requestObj, &leaseObjHandler)
			if err != nil {
				log.Error(err)
				responseObj.Status = err.Error()
			} else {
				responseObj.Status = "Success"
			}
			res := prepareLeaseJsonResponse(requestObj, responseObj)
			writeToJson(res, leaseObjHandler.jsonFilePath)
		}
	}

	log.Info("-----END OF EXECUTION-----")
}
