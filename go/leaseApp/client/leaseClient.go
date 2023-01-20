package main

import (
	"bytes"
	"common/requestResponseLib"
	"encoding/gob"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"sync/atomic"

	pmdbClient "niova/go-pumicedb-lib/client"
	PumiceDBCommon "niova/go-pumicedb-lib/common"

	log "github.com/sirupsen/logrus"

	"github.com/google/uuid"
)

type state int

const (
	ACQUIRED      state = 0
	FREE                = 1
	TRANSITIONING       = 2
)

var (
	operationsMap = map[string]int{
		"GET":     requestResponseLib.GET,
		"PUT":     requestResponseLib.PUT,
		"LOOKUP":  requestResponseLib.LOOKUP,
		"REFRESH": requestResponseLib.REFRESH,
	}
)

type leaseHandler struct {
	raftUUID      uuid.UUID
	pmdbClientObj *pmdbClient.PmdbClientObj
	jsonFilePath  string
	logFilePath   string
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
	Status     int
	LeaseState string
	TTL        int
	TimeStamp  requestResponseLib.LeaderTS
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
	case requestResponseLib.GET:
		return "GET"
	case requestResponseLib.PUT:
		return "PUT"
	case requestResponseLib.LOOKUP:
		return "LOOKUP"
	case requestResponseLib.REFRESH:
		return "REFRESH"
	}
	return "UNKNOWN"
}

func getStringLeaseState(leaseState int) string {
	switch leaseState {
	case requestResponseLib.GRANTED:
		return "GRANTED"
	case requestResponseLib.INPROGRESS:
		return "IN-PROGRESS"
	case requestResponseLib.EXPIRED:
		return "EXPIRED"
	case requestResponseLib.AIU:
		return "ALREADY-IN-USE"
	case requestResponseLib.INVALID:
		return "INVALID"
	}
	return "UNKNOWN"
}

func prepareJsonResponse(requestObj requestResponseLib.LeaseReq, responseObj requestResponseLib.LeaseStruct) writeObj {
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
func (handler *leaseHandler) getCmdParams() requestResponseLib.LeaseReq {
	var stringOperation, strClientUUID, strResourceUUID, strRaftUUID string
	var requestObj requestResponseLib.LeaseReq
	var tempOperation int
	var ok bool
	var err error

	flag.StringVar(&strClientUUID, "u", "NULL", "ClientUUID - UUID of the requesting client")
	flag.StringVar(&strResourceUUID, "v", "NULL", "ResourceUUID - UUID of the requested resource")
	flag.StringVar(&strRaftUUID, "ru", "NULL", "RaftUUID - UUID of the raft cluster")
	flag.StringVar(&handler.jsonFilePath, "j", "/tmp", "Output file path")
	flag.StringVar(&handler.logFilePath, "l", "", "Log file path")
	flag.StringVar(&stringOperation, "o", "LOOKUP", "Operation - GET/PUT/LOOKUP/REFRESH")
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
	handler.raftUUID, err = uuid.Parse(strRaftUUID)
	if err != nil {
		usage()
		os.Exit(-1)
	}
	requestObj.Client, err = uuid.Parse(strClientUUID)
	if err != nil {
		usage()
		os.Exit(-1)
	}
	requestObj.Resource, err = uuid.Parse(strResourceUUID)
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
	handler.pmdbClientObj.AppUUID = uuid.New().String()
	return nil
}

/*
Structure : leaseHandler
Method	  : Write()
Arguments : LeaseReq, rncui, *LeaseResp
Return(s) : error

Description : Wrapper function for WriteEncoded() function
*/

func (handler *leaseHandler) Write(requestObj requestResponseLib.LeaseReq, rncui string, response *[]byte) error {
	var err error
	var requestBytes bytes.Buffer
	enc := gob.NewEncoder(&requestBytes)
	err = enc.Encode(requestObj)
	if err != nil {
		log.Error("Encoding error : ", err)
		return err
	}
	err = handler.pmdbClientObj.WriteEncodedAndGetResponse(requestBytes.Bytes(), rncui, 1, response)

	return err
}

/*
Structure : leaseHandler
Method	  : Read()
Arguments : LeaseReq, rncui, *response
Return(s) : error

Description : Wrapper function for ReadEncoded() function
*/
func (handler *leaseHandler) Read(requestObj requestResponseLib.LeaseReq, rncui string, response *[]byte) error {
	var err error
	var requestBytes bytes.Buffer
	enc := gob.NewEncoder(&requestBytes)
	err = enc.Encode(requestObj)
	if err != nil {
		log.Error("Encoding error : ", err)
		return err
	}
	return handler.pmdbClientObj.ReadEncoded(requestBytes.Bytes(), rncui, response)
}

/*
Structure : leaseHandler
Method	  : get()
Arguments : requestResponseLib.LeaseReq
Return(s) : error

Description : Handler function for get() operation
              Acquire a lease on a particular resource
*/
func (handler *leaseHandler) get(requestObj requestResponseLib.LeaseReq) error {
	var err error
	var responseBytes []byte
	var responseObj requestResponseLib.LeaseStruct

	rncui := handler.getRNCUI()

	err = handler.Write(requestObj, rncui, &responseBytes)
	if err != nil {
		log.Error(err)
	}

	dec := gob.NewDecoder(bytes.NewBuffer(responseBytes))
	err = dec.Decode(&responseObj)

	log.Info("Write request status - ", responseObj.Status)
	res := prepareJsonResponse(requestObj, responseObj)
	handler.writeToJson(res, handler.jsonFilePath)

	return err
}

/*
Structure : leaseHandler
Method	  : lookup()
Arguments : requestResponseLib.LeaseReq
Return(s) : error

Description : Handler function for lookup() operation
              Lookup lease info of a particular resource
*/
func (handler *leaseHandler) lookup(requestObj requestResponseLib.LeaseReq) error {
	var err error
	var responseBytes []byte

	err = handler.Read(requestObj, "", &responseBytes)
	if err != nil {
		log.Error(err)
	}

	leaseObj := requestResponseLib.LeaseStruct{}
	dec := gob.NewDecoder(bytes.NewBuffer(responseBytes))
	err = dec.Decode(&leaseObj)
	if err != nil {
		log.Error("Decoding error : ", err)
		leaseObj.Status = -1
	} else {
		leaseObj.Status = 0
	}
	res := prepareJsonResponse(requestObj, leaseObj)
	handler.writeToJson(res, handler.jsonFilePath)

	return err
}

/*
Structure : leaseHandler
Method	  : refresh()
Arguments : requestResponseLib.LeaseReq
Return(s) : error

Description : Handler function for refresh() operation
              Refresh lease of a owned resource
*/
func (handler *leaseHandler) refresh(requestObj requestResponseLib.LeaseReq) error {
	var err error
	var responseBytes []byte
	var responseObj requestResponseLib.LeaseStruct

	rncui := handler.getRNCUI()
	err = handler.Write(requestObj, rncui, &responseBytes)
	if err != nil {
		log.Error(err)
	}

	dec := gob.NewDecoder(bytes.NewBuffer(responseBytes))
	err = dec.Decode(&responseObj)

	log.Info("Refresh request status - ", responseObj.Status)

	res := prepareJsonResponse(requestObj, responseObj)
	handler.writeToJson(res, handler.jsonFilePath)

	return err
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
	switch requestObj.Operation {
	case requestResponseLib.GET:
		// get lease
		err := leaseObjHandler.get(requestObj)
		if err != nil {
			log.Error(err)
		}
	case requestResponseLib.LOOKUP:
		// lookup lease
		err := leaseObjHandler.lookup(requestObj)
		if err != nil {
			log.Error(err)
		}
	case requestResponseLib.REFRESH:
		// refresh lease
		err := leaseObjHandler.refresh(requestObj)
		if err != nil {
			log.Error(err)
		}
	}

	log.Info("-----END OF EXECUTION-----")
}
