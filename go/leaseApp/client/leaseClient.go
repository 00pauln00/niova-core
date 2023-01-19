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

// will be used to write req and res to json file
type writeObj struct {
	Request  JsonLeaseReq
	Response requestResponseLib.LeaseStruct
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
		return "RESFRESH"
	}
	return "UNKNOWN"
}

func prepareJsonResponse(requestObj requestResponseLib.LeaseReq, responseObj requestResponseLib.LeaseStruct) writeObj {
	req := JsonLeaseReq{
		Client:    requestObj.Client,
		Resource:  requestObj.Resource,
		Operation: getStringOperation(requestObj.Operation),
	}
	res := writeObj{
		Request:  req,
		Response: responseObj,
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
	flag.StringVar(&handler.logFilePath, "l", "./", "Log file path")
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
Method	  : WriteLease()
Arguments : LeaseReq, rncui, *LeaseResp
Return(s) : error

Description : Wrapper function for WriteEncoded() function
*/

func (handler *leaseHandler) WriteLease(requestObj requestResponseLib.LeaseReq, rncui string, response *[]byte) error {
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
Method	  : ReadLease()
Arguments : LeaseReq, rncui, *response
Return(s) : error

Description : Wrapper function for ReadEncoded() function
*/
func (handler *leaseHandler) ReadLease(requestObj requestResponseLib.LeaseReq, rncui string, response *[]byte) error {
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
Method	  : get_lease()
Arguments : requestResponseLib.LeaseReq
Return(s) : error

Description : Handler function for get_lease() operation
              Acquire a lease on a particular resource
*/
func (handler *leaseHandler) get_lease(requestObj requestResponseLib.LeaseReq) error {
	var err error
	var responseBytes []byte
	var responseObj requestResponseLib.LeaseStruct

	rncui := handler.getRNCUI()

	err = handler.WriteLease(requestObj, rncui, &responseBytes)
	if err != nil {
		log.Error(err)
	}

	dec := gob.NewDecoder(bytes.NewBuffer(responseBytes))
	err = dec.Decode(&responseObj)

	log.Info("Write request status - ", responseObj.Status)
	res := prepareJsonResponse(requestObj, responseObj)
	handler.writeToJson(res, handler.logFilePath)

	return err
}

/*
Structure : leaseHandler
Method	  : lookup_lease()
Arguments : requestResponseLib.LeaseReq
Return(s) : error

Description : Handler function for lookup_lease() operation
              Lookup lease info of a particular resource
*/
func (handler *leaseHandler) lookup_lease(requestObj requestResponseLib.LeaseReq) error {
	var err error
	var responseBytes []byte

	err = handler.ReadLease(requestObj, "", &responseBytes)
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
	handler.writeToJson(res, handler.logFilePath)

	return err
}

/*
Structure : leaseHandler
Method	  : refresh_lease()
Arguments : requestResponseLib.LeaseReq
Return(s) : error

Description : Handler function for refresh_lease() operation
              Refresh lease of a owned resource
*/
func (handler *leaseHandler) refresh_lease(requestObj requestResponseLib.LeaseReq) error {
	var err error
	var responseBytes []byte
	var responseObj requestResponseLib.LeaseStruct

	rncui := handler.getRNCUI()
	err = handler.WriteLease(requestObj, rncui, &responseBytes)
	if err != nil {
		log.Error(err)
	}

	log.Info("Refresh request status - ", responseObj.Status)

	res := prepareJsonResponse(requestObj, responseObj)
	handler.writeToJson(res, handler.logFilePath)

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
		err := leaseObjHandler.get_lease(requestObj)
		if err != nil {
			log.Error(err)
		}
	case requestResponseLib.LOOKUP:
		// lookup lease
		err := leaseObjHandler.lookup_lease(requestObj)
		if err != nil {
			log.Error(err)
		}
	case requestResponseLib.REFRESH:
		// refresh lease
		err := leaseObjHandler.refresh_lease(requestObj)
		if err != nil {
			log.Error(err)
		}
	}

	log.Info("-----END OF EXECUTION-----")
}
