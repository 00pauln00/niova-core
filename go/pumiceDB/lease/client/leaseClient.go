package leaseClient

import (
	"bytes"
	"common/leaseLib"
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
		"GET":     leaseLib.GET,
		"PUT":     leaseLib.PUT,
		"LOOKUP":  leaseLib.LOOKUP,
		"REFRESH": leaseLib.REFRESH,
	}
)

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

type LeaseHandler struct {
	RaftUUID      uuid.UUID
	PmdbClientObj *pmdbClient.PmdbClientObj
	JsonFilePath  string
	LogFilePath   string
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

func (handler LeaseHandler) getRNCUI() string {
	idq := atomic.AddUint64(&handler.PmdbClientObj.WriteSeqNo, uint64(1))
	rncui := fmt.Sprintf("%s:0:0:0:%d", handler.PmdbClientObj.AppUUID, idq)
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
Structure : LeaseHandler
Method	  : getCmdParams
Arguments : None
Return(s) : None

Description : Parse command line params and load into leaseHandler sturct
*/
func (handler LeaseHandler) getCmdParams() leaseLib.LeaseReq {
	var stringOperation, strClientUUID, strResourceUUID, strRaftUUID string
	var requestObj leaseLib.LeaseReq
	var tempOperation int
	var ok bool
	var err error

	flag.StringVar(&strClientUUID, "u", "NULL", "ClientUUID - UUID of the requesting client")
	flag.StringVar(&strResourceUUID, "v", "NULL", "ResourceUUID - UUID of the requested resource")
	flag.StringVar(&strRaftUUID, "ru", "NULL", "RaftUUID - UUID of the raft cluster")
	flag.StringVar(&handler.JsonFilePath, "j", "/tmp", "Output file path")
	flag.StringVar(&handler.LogFilePath, "l", "", "Log file path")
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
	handler.RaftUUID, err = uuid.Parse(strRaftUUID)
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
Structure : LeaseHandler
Method	  : startPMDBClient
Arguments : None
Return(s) : error

Description : Start PMDB Client object for ClientUUID and RaftUUID
*/
func (handler LeaseHandler) startPMDBClient(client string) error {
	var err error

	//Get clientObj
	log.Info("Raft UUID - ", handler.RaftUUID.String(), " Client UUID - ", client)
	handler.PmdbClientObj = pmdbClient.PmdbClientNew(handler.RaftUUID.String(), client)
	if handler.PmdbClientObj == nil {
		return errors.New("PMDB Client Obj could not be initialized")
	}

	//Start PMDB Client
	err = handler.PmdbClientObj.Start()
	if err != nil {
		return err
	}

	leaderUuid, err := handler.PmdbClientObj.PmdbGetLeader()
	for err != nil {
		leaderUuid, err = handler.PmdbClientObj.PmdbGetLeader()
	}
	log.Info("Leader uuid : ", leaderUuid.String())

	//Store encui in AppUUID
	handler.PmdbClientObj.AppUUID = uuid.New().String()
	return nil
}

/*
Structure : LeaseHandler
Method	  : write()
Arguments : LeaseReq, rncui, *LeaseResp
Return(s) : error

Description : Wrapper function for WriteEncoded() function
*/

func (handler LeaseHandler) write(requestObj leaseLib.LeaseReq, rncui string, response *[]byte) error {
	var err error
	var requestBytes bytes.Buffer
	enc := gob.NewEncoder(&requestBytes)
	err = enc.Encode(requestObj)
	if err != nil {
		return err
	}
	err = handler.PmdbClientObj.WriteEncodedAndGetResponse(requestBytes.Bytes(), rncui, 1, response)

	return err
}

/*
Structure : LeaseHandler
Method	  : read()
Arguments : LeaseReq, rncui, *response
Return(s) : error

Description : Wrapper function for ReadEncoded() function
*/
func (handler LeaseHandler) read(requestObj leaseLib.LeaseReq, rncui string, response *[]byte) error {
	var err error
	var requestBytes bytes.Buffer
	enc := gob.NewEncoder(&requestBytes)
	err = enc.Encode(requestObj)
	if err != nil {
		return err
	}
	return handler.PmdbClientObj.ReadEncoded(requestBytes.Bytes(), rncui, response)
}

/*
Structure : LeaseHandler
Method	  : get()
Arguments : leaseLib.LeaseReq
Return(s) : error

Description : Handler function for get() operation
              Acquire a lease on a particular resource
*/
func (handler LeaseHandler) Get(requestObj leaseLib.LeaseReq) (leaseLib.LeaseStruct, error) {
	var err error
	var responseBytes []byte
	var responseObj leaseLib.LeaseStruct

	rncui := handler.getRNCUI()

	err = handler.write(requestObj, rncui, &responseBytes)
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
Structure : LeaseHandler
Method	  : lookup()
Arguments : leaseLib.LeaseReq
Return(s) : error

Description : Handler function for lookup() operation
              Lookup lease info of a particular resource
*/
func (handler LeaseHandler) Lookup(requestObj leaseLib.LeaseReq) (leaseLib.LeaseStruct, error) {
	var err error
	var responseBytes []byte
	var responseObj leaseLib.LeaseStruct

	err = handler.read(requestObj, "", &responseBytes)
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
Structure : LeaseHandler
Method	  : refresh()
Arguments : leaseLib.LeaseReq
Return(s) : error

Description : Handler function for refresh() operation
              Refresh lease of a owned resource
*/
func (handler LeaseHandler) Refresh(requestObj leaseLib.LeaseReq) (leaseLib.LeaseStruct, error) {
	var err error
	var responseBytes []byte
	var responseObj leaseLib.LeaseStruct

	rncui := handler.getRNCUI()
	err = handler.write(requestObj, rncui, &responseBytes)
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
Structure : LeaseHandler
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
	leaseObjHandler := LeaseHandler{}

	// Load cmd params
	requestObj := leaseObjHandler.getCmdParams()

	/*
		Initialize Logging
	*/
	err := PumiceDBCommon.InitLogger(leaseObjHandler.LogFilePath)
	if err != nil {
		log.Error("Error while initializing the logger ", err)
	}

	err = leaseObjHandler.startPMDBClient(requestObj.Client.String())
	if err != nil {
		log.Error(err)
		os.Exit(-1)
	}
	var responseObj leaseLib.LeaseStruct
	switch requestObj.Operation {
	case leaseLib.GET:
		// get lease
		responseObj, err = leaseObjHandler.Get(requestObj)
		if err != nil {
			log.Error(err)
			responseObj.Status = err.Error()
		} else {
			responseObj.Status = "Success"
		}
	case leaseLib.LOOKUP:
		// lookup lease
		responseObj, err = leaseObjHandler.Lookup(requestObj)
		if err != nil {
			log.Error(err)
			responseObj.Status = err.Error()
		} else {
			responseObj.Status = "Success"
		}
	case leaseLib.REFRESH:
		// refresh lease
		responseObj, err = leaseObjHandler.Refresh(requestObj)
		if err != nil {
			log.Error(err)
			responseObj.Status = err.Error()
		} else {
			responseObj.Status = "Success"
		}
	}
	res := prepareJsonResponse(requestObj, responseObj)
	writeToJson(res, leaseObjHandler.JsonFilePath)

	log.Info("-----END OF EXECUTION-----")
}
