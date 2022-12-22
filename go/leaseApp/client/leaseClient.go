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
	"time"

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
	client        uuid.UUID
	resource      uuid.UUID
	raftUUID      uuid.UUID
	ttl           time.Duration
	pmdbClientObj *pmdbClient.PmdbClientObj
	operation     operation
	leaseState    state
	timeStamp     string
	jsonFilePath  string
}

type hybridTS struct {
	Major uint32
	Minor uint64
}

type leaseStruct struct {
	Resource     uuid.UUID
	Client       uuid.UUID
	Status       int
	LeaseGranted hybridTS
	LeaseExpiry  hybridTS
}

func usage() {
	flag.PrintDefaults()
	os.Exit(0)
}

func parseOperation(str string) (operation, bool) {
	op, ok := operationsMap[str]
	return op, ok
}

/*
Structure : leaseHandler
Method	  : getCmdParams
Arguments : None
Return(s) : None

Description : Parse command line params and load into leaseHandler sturct
*/
func (handler *leaseHandler) getCmdParams() {
	var stringOperation, strClientUUID, strResourceUUID, strRaftUUID string
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
	handler.client, err = uuid.Parse(strClientUUID)
	if err != nil {
		usage()
		os.Exit(-1)
	}
	handler.resource, err = uuid.Parse(strResourceUUID)
	if err != nil {
		usage()
		os.Exit(-1)
	}
	handler.raftUUID, err = uuid.Parse(strRaftUUID)
	if err != nil {
		usage()
		os.Exit(-1)
	}
}

/*
Structure : leaseHandler
Method	  : startPMDBClient
Arguments : None
Return(s) : error

Description : Start PMDB Client object for ClientUUID and RaftUUID
*/
func (handler *leaseHandler) startPMDBClient() error {
	var err error

	//Get clientObj
	log.Info("Raft UUID - ", handler.raftUUID.String(), " Client UUID - ", handler.client.String())
	handler.pmdbClientObj = pmdbClient.PmdbClientNew(handler.raftUUID.String(), handler.client.String())
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

func (handler *leaseHandler) WriteCallBack(request []byte, rncui string, response *[]byte) error {
	var err error
	requestObj := requestResponseLib.LeaseReq{}
	dec := gob.NewDecoder(bytes.NewBuffer(request))
	err = dec.Decode(&requestObj)
	if err != nil {
		return err
	}

	err = handler.pmdbClientObj.WriteEncoded(request, rncui)
	var responseObj requestResponseLib.LeaseResp
	if err != nil {
		responseObj.Status = "Failed"
		log.Error(err)
	} else {
		responseObj.Status = "Success"
	}

	var responseBuffer bytes.Buffer
	enc := gob.NewEncoder(&responseBuffer)
	err = enc.Encode(responseObj)
	*response = responseBuffer.Bytes()
	return err
}

func (handler *leaseHandler) ReadCallBack(request []byte, rncui string, response *[]byte) error {
	return handler.pmdbClientObj.ReadEncoded(request, rncui, response)
}

/*
Structure : leaseHandler
Method	  : sendReq
Arguments : LeaseReq
Return(s) : error

Description : Send read/get/refresh request to server
*/
//TODO Need to remove this function and add wrappers for write/read/refresh callbacks instead
func (handler *leaseHandler) sendReq(req *requestResponseLib.LeaseReq) error {
	var err error
	var responseObj requestResponseLib.LeaseResp
	var requestBytes bytes.Buffer
	var responseBytes []byte
	enc := gob.NewEncoder(&requestBytes)
	err = enc.Encode(req)
	if err != nil {
		log.Error("Encoding error : ", err)
		return err
	}
	idq := atomic.AddUint64(&handler.pmdbClientObj.WriteSeqNo, uint64(1))
	rncui := fmt.Sprintf("%s:0:0:0:%d", handler.pmdbClientObj.AppUUID, idq)

	switch handler.operation {
	case GET:
		//Request lease
		err = handler.WriteCallBack(requestBytes.Bytes(), rncui, &responseBytes)
		if err != nil {
			log.Error("Error while sending the request : ", err)
			return err
		}
		//TODO Add decoing to a function and reuse for following cases
		dec := gob.NewDecoder(bytes.NewBuffer(responseBytes))
		err = dec.Decode(&responseObj)
		if err != nil {
			log.Error("Decoding error : ", err)
			return err
		}
		log.Info("Write Req Status - ", responseObj.Status)

	case LOOKUP:
		//Request lookup
		leaseObj := leaseStruct{}
		err = handler.ReadCallBack(requestBytes.Bytes(), "", &responseBytes)
		if err != nil {
			log.Error("Error while sending the request : ", err)
			return err
		}
		dec := gob.NewDecoder(bytes.NewBuffer(responseBytes))
		err = dec.Decode(&leaseObj)
		if err != nil {
			log.Error("Decoding error : ", err)
			return err
		}
		fmt.Println("Status - ", leaseObj.Status)
		fmt.Println("Lease Owner - ", leaseObj.Client)
		fmt.Println("Resource UUID - ", leaseObj.Resource)
		fmt.Println("Lease Granted at - ", leaseObj.LeaseGranted)
		fmt.Println("Lease Expiry at - ", leaseObj.LeaseExpiry)

	case REFRESH:
		//responseBytes, err = handler.pmdbClientObj.RefreshLease(requestBytes.Bytes(), "", true)
		/*
			if err != nil {
				log.Error("Error while sending the request : ", err)
				return nil, err
			}
		*/
	}
	//Decode the req response
	return err
}

/*
Structure : leaseHandler
Method	  : get_lease()
Arguments :
Return(s) : error

Description : Handler function for get_lease() operation
              Acquire a lease on a particular resource
*/
func (handler *leaseHandler) get_lease() error {

	requestObj := requestResponseLib.LeaseReq{
		Client:   handler.client,
		Resource: handler.resource,
	}

	err := handler.sendReq(&requestObj)
	if err != nil {
		log.Error(err)
	}

	//handler.writeToJson(response)

	return err
}

/*
Structure : leaseHandler
Method	  : lookup_lease()
Arguments :
Return(s) : error

Description : Handler function for lookup_lease() operation
              Lookup lease info of a particular resource
*/
func (handler *leaseHandler) lookup_lease() error {
	requestObj := requestResponseLib.LeaseReq{
		Client:   handler.client,
		Resource: handler.resource,
	}

	err := handler.sendReq(&requestObj)
	if err != nil {
		log.Error(err)
	}
	//handler.writeToJson(response)

	return err
}

/*
Structure : leaseHandler
Method	  : refresh_lease()
Arguments :
Return(s) : error

Description : Handler function for refresh_lease() operation
              Refresh lease of a owned resource
*/
func (handler *leaseHandler) refresh_lease() error {
	requestObj := requestResponseLib.LeaseReq{
		Client:   handler.client,
		Resource: handler.resource,
	}

	err := handler.sendReq(&requestObj)
	if err != nil {
		log.Error(err)
	}
	//handler.writeToJson(response)

	return err
}

/*
Structure : leaseHandler
Method	  : writeToJson
Arguments :
Return(s) : error

Description : Write response/error to json file
*/
func (handler *leaseHandler) writeToJson(response requestResponseLib.LeaseResp) error {
	var err error

	tempOutfileName := handler.jsonFilePath + "/" + handler.raftUUID.String() + ".json"
	file, _ := json.MarshalIndent(response, "", "\t")
	_ = ioutil.WriteFile(tempOutfileName, file, 0644)

	return err
}

func main() {
	leaseObj := leaseHandler{}

	// Load cmd params
	leaseObj.getCmdParams()

	/*
		Initialize Logging
	*/

	err := leaseObj.startPMDBClient()
	if err != nil {
		log.Error(err)
		os.Exit(-1)
	}

	switch leaseObj.operation {
	case GET:
		// get lease
		err := leaseObj.get_lease()
		if err != nil {
			log.Error(err)
		}
	case LOOKUP:
		// lookup lease
		err := leaseObj.lookup_lease()
		if err != nil {
			log.Error(err)
		}
	case REFRESH:
		// refresh lease
		err := leaseObj.refresh_lease()
		if err != nil {
			log.Error(err)
		}
	}

	// Write response to json file
	//leaseObj.writeToJson()

	log.Info("-----END OF EXECUTION-----")
}
