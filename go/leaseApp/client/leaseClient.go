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

	log "github.com/sirupsen/logrus"

	"github.com/google/uuid"
)

type operation int
type state int

const (
	GET     operation = 0
	PUT               = 1
	LOOKUP            = 2
	REFRESH           = 3
)

const (
	ACQUIRED      state = 0
	FREE                = 1
	TRANSITIONING       = 2
)

var (
	operationsMap = map[string]operation{
		"GET":     GET,
		"PUT":     PUT,
		"LOOKUP":  LOOKUP,
		"REFRESH": REFRESH,
	}
)

type leaseHandler struct {
	//client        uuid.UUID
	//resource      uuid.UUID
	raftUUID uuid.UUID
	//ttl           time.Duration
	pmdbClientObj *pmdbClient.PmdbClientObj
	operation     operation
	//leaseState    state
	//timeStamp     string
	jsonFilePath string
}

func usage() {
	flag.PrintDefaults()
	os.Exit(0)
}

func parseOperation(str string) (operation, bool) {
	op, ok := operationsMap[str]
	return op, ok
}

func (handler *leaseHandler) getRNCUI() string {
	idq := atomic.AddUint64(&handler.pmdbClientObj.WriteSeqNo, uint64(1))
	rncui := fmt.Sprintf("%s:0:0:0:%d", handler.pmdbClientObj.AppUUID, idq)
	return rncui
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
	var ok bool
	var err error

	flag.StringVar(&strClientUUID, "u", "NULL", "ClientUUID - UUID of the requesting client")
	flag.StringVar(&strResourceUUID, "v", "NULL", "ResourceUUID - UUID of the requested resource")
	flag.StringVar(&strRaftUUID, "ru", "NULL", "RaftUUID - UUID of the raft cluster")
	flag.StringVar(&handler.jsonFilePath, "j", "/tmp", "Output file path")
	flag.StringVar(&stringOperation, "o", "LOOKUP", "Operation - GET/PUT/LOOKUP/REFRESH")
	flag.Usage = usage
	flag.Parse()
	if flag.NFlag() == 0 {
		usage()
		os.Exit(-1)
	}
	handler.operation, ok = parseOperation(stringOperation)
	if !ok {
		usage()
		os.Exit(-1)
	}
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
Method	  : WriteCallBack()
Arguments : LeaseReq, rncui, *LeaseResp
Return(s) : error

Description : Wrapper function for WriteEncoded() function
*/

func (handler *leaseHandler) WriteCallBack(requestObj requestResponseLib.LeaseReq, rncui string, response *requestResponseLib.LeaseResp) error {
	var err error
	var requestBytes bytes.Buffer
	enc := gob.NewEncoder(&requestBytes)
	err = enc.Encode(requestObj)
	if err != nil {
		log.Error("Encoding error : ", err)
		return err
	}
	err = handler.pmdbClientObj.WriteEncoded(requestBytes.Bytes(), rncui)
	var responseObj requestResponseLib.LeaseResp
	if err != nil {
		responseObj.Status = "Failed"
		log.Error(err)
	} else {
		responseObj.Status = "Success"
	}

	return err
}

/*
Structure : leaseHandler
Method	  : ReadCallBack()
Arguments : LeaseReq, rncui, *response
Return(s) : error

Description : Wrapper function for ReadEncoded() function
*/
func (handler *leaseHandler) ReadCallBack(requestObj requestResponseLib.LeaseReq, rncui string, response *[]byte) error {
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
	var responseObj requestResponseLib.LeaseResp

	rncui := handler.getRNCUI()
	err = handler.WriteCallBack(requestObj, rncui, &responseObj)
	if err != nil {
		log.Error(err)
	}

	log.Info("Write request status - ", responseObj.Status)
	handler.writeToJson(responseObj)

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
	var responseBytes []byte
	var err error

	err = handler.ReadCallBack(requestObj, "", &responseBytes)
	if err != nil {
		log.Error(err)
	}

	leaseObj := requestResponseLib.LeaseStruct{}
	dec := gob.NewDecoder(bytes.NewBuffer(responseBytes))
	err = dec.Decode(&leaseObj)
	if err != nil {
		log.Error("Decoding error : ", err)
	}
	handler.writeToJson(leaseObj)

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
	var responseObj requestResponseLib.LeaseResp

	rncui := handler.getRNCUI()
	err = handler.WriteCallBack(requestObj, rncui, &responseObj)
	if err != nil {
		log.Error(err)
	}

	log.Info("Refresh request status - ", responseObj.Status)
	handler.writeToJson(responseObj)

	return err
}

/*
Structure : leaseHandler
Method	  : writeToJson
Arguments : struct
Return(s) : error

Description : Write response/error to json file
*/
func (handler *leaseHandler) writeToJson(toJson interface{}) {
	file, err := json.MarshalIndent(toJson, "", " ")
	err = ioutil.WriteFile(handler.jsonFilePath+".json", file, 0644)
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

	err := leaseObjHandler.startPMDBClient(requestObj.Client.String())
	if err != nil {
		log.Error(err)
		os.Exit(-1)
	}
	switch leaseObjHandler.operation {
	case GET:
		// get lease
		err := leaseObjHandler.get_lease(requestObj)
		if err != nil {
			log.Error(err)
		}
		err = leaseObjHandler.lookup_lease(requestObj)
		if err != nil {
			log.Error(err)
		}
	case LOOKUP:
		// lookup lease
		err := leaseObjHandler.lookup_lease(requestObj)
		if err != nil {
			log.Error(err)
		}
	case REFRESH:
		// refresh lease
		err := leaseObjHandler.refresh_lease(requestObj)
		if err != nil {
			log.Error(err)
		}
	}

	log.Info("-----END OF EXECUTION-----")
}
