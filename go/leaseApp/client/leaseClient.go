package main

import (
	"bytes"
	"common/requestResponseLib"
	"encoding/gob"
	"encoding/json"
	"errors"
	"flag"
	"io/ioutil"
	"os"
	"time"

	pmdbClient "niova/go-pumicedb-lib/client"

	log "github.com/sirupsen/logrus"

	"github.com/google/uuid"
)

type operation int

const (
	GET     operation = 0
	PUT               = 1
	LOOKUP            = 2
	REFRESH           = 3
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
	state         int
	timeStamp     string
	jsonFilePath  string
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
	var stringOperation, tempClientUUID, tempVdevUUID, tempRaftUUID string
	var ok bool
	var err error

	flag.StringVar(&tempClientUUID, "u", uuid.New().String(), "ClientUUID - UUID of the requesting client")
	flag.StringVar(&tempVdevUUID, "v", "NULL", "VdevUUID - UUID of the requested VDEV")
	flag.StringVar(&tempRaftUUID, "ru", "NULL", "RaftUUID - UUID of the raft cluster")
	flag.StringVar(&handler.jsonFilePath, "j", "/tmp", "Output file path")
	flag.StringVar(&stringOperation, "o", "LOOKUP", "Operation - GET/PUT/LOOKUP/REFRESH")
	flag.Usage = usage
	flag.Parse()
	if flag.NFlag() == 0 {
		usage()
		os.Exit(-1)
	}
	/*
		handler.operation, err = strconv.Atoi(stringOperation)
		if err != nil {
			usage()
			os.Exit(-1)
		}
	*/
	handler.operation, ok = parseOperation(stringOperation)
	if !ok {
		usage()
		os.Exit(-1)
	}
	handler.client, err = uuid.Parse(tempClientUUID)
	if err != nil {
		usage()
		os.Exit(-1)
	}
	handler.resource, err = uuid.Parse(tempVdevUUID)
	if err != nil {
		usage()
		os.Exit(-1)
	}
	handler.raftUUID, err = uuid.Parse(tempRaftUUID)
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
	handler.pmdbClientObj = pmdbClient.PmdbClientNew(handler.raftUUID.String(), handler.client.String())
	if handler.pmdbClientObj == nil {
		return errors.New("PMDB Client Obj could not be initialized")
	}

	//Start PMDB Client
	err = handler.pmdbClientObj.Start()
	if err != nil {
		return err
	}

	//Store encui in AppUUID
	handler.pmdbClientObj.AppUUID = uuid.New().String()
	return nil
}

/*
Structure : leaseHandler
Method	  : sendReq
Arguments : LeaseReq
Return(s) : error

Description : Send read/get/refresh request to server
*/
func (handler *leaseHandler) sendReq(req *requestResponseLib.LeaseReq) (requestResponseLib.LeaseResp, error) {
	var err error
	var responseObj requestResponseLib.LeaseResp
	var requestBytes bytes.Buffer
	var responseBytes []byte
	enc := gob.NewEncoder(&requestBytes)
	err = enc.Encode(req)
	if err != nil {
		log.Error("Encoding error : ", err)
		return responseObj, err
	}

	switch handler.operation {
	case GET:
		//Request lease
		/*
			responseBytes, err = handler.pmdbClientObj.GetLease(requestBytes.Bytes(), "", true)
			if err != nil {
				log.Error("Error while sending the request : ", err)
				return nil, err
			}
		*/
	case LOOKUP:
		//Request lookup
		/*
			responseBytes, err = handler.pmdbClientObj.LookupLease(requestBytes.Bytes(), "", true)
			if err != nil {
				log.Error("Error while sending the request : ", err)
				return nil, err
			}
		*/
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
	dec := gob.NewDecoder(bytes.NewBuffer(responseBytes))
	err = dec.Decode(&responseObj)
	if err != nil {
		log.Error("Decoding error : ", err)
		return responseObj, err
	}
	return responseObj, err
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
		Client:    handler.client,
		Resource:  handler.resource,
		Operation: int(handler.operation),
	}

	response, err := handler.sendReq(&requestObj)
	if err != nil {
		log.Error(err)
	}

	handler.writeToJson(response)

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
		Resource:  handler.resource,
		Operation: int(handler.operation),
	}

	response, err := handler.sendReq(&requestObj)
	if err != nil {
		log.Error(err)
	}
	handler.writeToJson(response)

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
		Client:    handler.client,
		Resource:  handler.resource,
		Operation: int(handler.operation),
	}

	response, err := handler.sendReq(&requestObj)
	if err != nil {
		log.Error(err)
	}
	handler.writeToJson(response)

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
