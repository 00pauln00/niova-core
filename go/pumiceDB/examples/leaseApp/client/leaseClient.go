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
)

type leaseHandler struct {
	clientObj    leaseClientLib.LeaseClient
	cliRequest   leaseClientLib.LeaseClientReqHandler
	cliOperation int
	jsonFilePath string
	logFilePath  string
	numOfLeases  int
	readJsonFile string
}

func usage() {
	flag.PrintDefaults()
	os.Exit(0)
}

func parseOperation(str string) (int, bool) {
	op, ok := operationsMap[str]
	return op, ok
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

func getRNCUI(clientObj *pmdbClient.PmdbClientObj) string {
	idq := atomic.AddUint64(&clientObj.WriteSeqNo, uint64(1))
	rncui := fmt.Sprintf("%s:0:0:0:%d", clientObj.AppUUID, idq)
	return rncui
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
	var tempOperation int
	var ok bool
	var err error

	flag.StringVar(&strClientUUID, "u", uuid.NewV4().String(), "ClientUUID - UUID of the requesting client")
	flag.StringVar(&strResourceUUID, "v", "", "ResourceUUID - UUID of the requested resource")
	flag.StringVar(&strRaftUUID, "ru", "NULL", "RaftUUID - UUID of the raft cluster")
	flag.StringVar(&handler.jsonFilePath, "j", "/tmp", "Output file path")
	flag.StringVar(&handler.logFilePath, "l", "", "Log file path")
	flag.StringVar(&stringOperation, "o", "LOOKUP", "Operation - GET/PUT/LOOKUP/REFRESH/GET_VALIDATE/LOOKUP_VALIDATE")
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
	handler.cliOperation = int(tempOperation)
	handler.clientObj.RaftUUID, err = uuid.FromString(strRaftUUID)
	if err != nil {
		usage()
		os.Exit(-1)
	}
	handler.cliRequest.LeaseReq.Client, err = uuid.FromString(strClientUUID)
	if err != nil {
		usage()
		os.Exit(-1)
	}
	if strResourceUUID != "" {
		handler.cliRequest.LeaseReq.Resource, err = uuid.FromString(strResourceUUID)
		if err != nil {
			usage()
			os.Exit(-1)
		}
	}
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

	handler.cliRequest.LeaseClientObj = &handler.clientObj

	//Store rncui in AppUUID
	handler.clientObj.PmdbClientObj.AppUUID = uuid.NewV4().String()
	return nil
}

/*
Description : Generate N number of client and resource uuids
*/

func (handler *leaseHandler) GetUuids() {
	//If user passed UUID through cmdline
	if handler.cliRequest.LeaseReq.Resource != uuid.Nil {
		handler.numOfLeases = 1
		kvMap[handler.cliRequest.LeaseReq.Client] = handler.cliRequest.LeaseReq.Resource
	} else {
		if handler.cliRequest.LeaseReq.Operation == leaseLib.GET ||
			handler.cliOperation == leaseLib.GET_VALIDATE {
			for i := 0; i < handler.numOfLeases; i++ {
				kvMap[uuid.NewV4()] = uuid.NewV4()
			}
		}
	}
}

func (handler *leaseHandler) write(requestBytes []byte, rncui string, response *[]byte) error {
	var err error
	var replySize int64

	//requestBytes := PreparePumiceReq(requestObj)
	reqArgs := &pmdbClient.PmdbReqArgs{
		Rncui:       rncui,
		ReqByteArr:  requestBytes,
		GetResponse: 1,
		ReplySize:   &replySize,
		Response:    response,
	}

	err = handler.clientObj.PmdbClientObj.WriteEncodedAndGetResponse(reqArgs)

	return err
}

func (handler *leaseHandler) read(requestBytes []byte, rncui string, response *[]byte) error {

	reqArgs := &pmdbClient.PmdbReqArgs{
		Rncui:      rncui,
		ReqByteArr: requestBytes,
		Response:   response,
	}

	return handler.clientObj.PmdbClientObj.ReadEncoded(reqArgs)
}

func (handler *leaseHandler) prepareLeaseRes(responseBytes *[]byte) error {
	var err error

	dec := gob.NewDecoder(bytes.NewBuffer(*responseBytes))
	err = dec.Decode(&handler.cliRequest.LeaseRes)
	if err != nil {
		return err
	}

	return err
}

/*
Description: Perform GET lease operation
*/

func (leaseHandler *leaseHandler) getLeases() error {

	leaseHandler.GetUuids()
	var response []leaseClientLib.LeaseClientReqHandler
	mapString := make(map[string]string)

	for key, value := range kvMap {
		var requestCli leaseClientLib.LeaseClientReqHandler
		var responseBytes []byte

		//prepare encoded pumiceReq with leaseReq as payload
		requestBytes := leaseClientLib.PrepareLeaseReq(key.String(), value.String(), leaseLib.GET)

		leaseHandler.cliRequest.Err = leaseHandler.write(requestBytes, getRNCUI(leaseHandler.clientObj.PmdbClientObj), &responseBytes)
		if leaseHandler.cliRequest.Err != nil {
			log.Error(leaseHandler.cliRequest.Err)
		}
		//prepare leaseRes
		leaseHandler.cliRequest.Err = leaseHandler.prepareLeaseRes(&responseBytes)
		if leaseHandler.cliRequest.Err != nil {
			log.Error(leaseHandler.cliRequest.Err)
		}

		requestCli = leaseHandler.cliRequest
		response = append(response, requestCli)
	}

	if leaseHandler.cliOperation == leaseLib.GET_VALIDATE && leaseHandler.numOfLeases >= 1 {
		//Check if prev element have same 'Status' and as current response.
		for i := 0; i < len(response); i++ {
			if response[i].LeaseRes.Status == "Success" {
				mapString["Status"] = "Success"
			} else {
				log.Info(" 'Status' not matched ")
				break
			}
		}
		// Write single 'Status' to json file.
		leaseHandler.writeSingleResponseToJson(mapString)
		// Write the detailed information to json file.
		leaseHandler.writeToJson(response)

	} else {
		// Write the detailed information to the json file.
		leaseHandler.writeToJson(response)
	}

	return leaseHandler.cliRequest.Err
}

/*
Description : Read JSON outfile and parse it.
*/

func getUuidFromFile(filename string) map[uuid.UUID]uuid.UUID {

	// read json file.
	file, _ := ioutil.ReadFile(filename + ".json")

	var resArr []leaseClientLib.LeaseClientReqHandler

	_ = json.Unmarshal([]byte(file), &resArr)

	for i := range resArr {
		kvMap[resArr[i].LeaseReq.Client] = resArr[i].LeaseReq.Resource
	}

	return kvMap
}

/*
Description: Perform LOOKUP lease operation
*/

func (leaseHandler *leaseHandler) lookupLeases() error {

	mapString := make(map[string]string)
	var response []leaseClientLib.LeaseClientReqHandler

	//If user not passed UUID through cmdline
	if leaseHandler.cliRequest.LeaseReq.Resource == uuid.Nil {
		//read get lease json outfile to extract client and resource uuid
		kvMap = getUuidFromFile(leaseHandler.readJsonFile)
	} else {
		leaseHandler.numOfLeases = 1
		kvMap[leaseHandler.cliRequest.LeaseReq.Client] = leaseHandler.cliRequest.LeaseReq.Resource
	}

	for key, value := range kvMap {
		//get pumiceReq encoded bytes
		requestBytes := leaseClientLib.PrepareLeaseReq(key.String(), value.String(), leaseHandler.cliOperation)

		//send encoded req to server
		var responseBytes []byte
		leaseHandler.cliRequest.Err = leaseHandler.read(requestBytes, "", &responseBytes)
		if leaseHandler.cliRequest.Err != nil {
			log.Error(leaseHandler.cliRequest.Err)
		}
		//prepare lease resp object
		leaseHandler.cliRequest.Err = leaseHandler.prepareLeaseRes(&responseBytes)
		if leaseHandler.cliRequest.Err != nil {
			log.Error(leaseHandler.cliRequest.Err)
		}

		response = append(response, leaseHandler.cliRequest)
	}

	if leaseHandler.cliOperation == leaseLib.LOOKUP_VALIDATE && leaseHandler.numOfLeases > 1 {
		//Check if prev element have same 'Status' and as current response.
		for i := 0; i < len(response); i++ {
			if response[i].LeaseRes.Status == "Success" {
				mapString["Status"] = "Success"
			} else {
				log.Info(" 'Status' not matched ")
				break
			}
			// Write single 'Status' to json file.
			leaseHandler.writeToJson(mapString)
		}
	} else {
		// Write the detailed response to the json file.
		leaseHandler.writeToJson(response)
	}

	return leaseHandler.cliRequest.Err
}

/*
Description: Perform REFRESH lease operation
*/

func (leaseHandler *leaseHandler) refreshLease() error {

	leaseHandler.cliRequest.LeaseReq.Rncui = getRNCUI(leaseHandler.clientObj.PmdbClientObj)
	leaseHandler.cliRequest.LeaseReq.Operation = leaseLib.REFRESH

	//prep req in pumiceReq format
	requestBytes := leaseClientLib.PrepareLeaseReq(leaseHandler.cliRequest.LeaseReq.Client.String(), leaseHandler.cliRequest.LeaseReq.Resource.String(), leaseLib.REFRESH)

	// send req
	var responseBytes []byte
	leaseHandler.cliRequest.Err = leaseHandler.write(requestBytes, getRNCUI(leaseHandler.clientObj.PmdbClientObj), &responseBytes)
	if leaseHandler.cliRequest.Err != nil {
		log.Error(leaseHandler.cliRequest.Err)
	}

	// prepare leaseRes
	leaseHandler.cliRequest.Err = leaseHandler.prepareLeaseRes(&responseBytes)
	if leaseHandler.cliRequest.Err != nil {
		log.Error(leaseHandler.cliRequest.Err)
	}
	// Write the response to the json file.
	leaseHandler.writeToJson(leaseHandler.cliRequest)
	return leaseHandler.cliRequest.Err
}

/*
Structure : leaseHandler
Method	  : writeToJson
Arguments : struct
Return(s) : error

Description : Write detailed request-response/error to json file
*/
func (leaseHandler *leaseHandler) writeToJson(toJson interface{}) {
	file, err := json.MarshalIndent(toJson, "", " ")
	err = ioutil.WriteFile(leaseHandler.jsonFilePath+".json", file, 0644)
	if err != nil {
		log.Error("Error writing to outfile : ", err)
	}
}

/*
Description : Write single response/error to json file
*/
func (leaseHandler *leaseHandler) writeSingleResponseToJson(toJson interface{}) {
	var filename string
	file, err := json.MarshalIndent(toJson, "", " ")
	filename = leaseHandler.jsonFilePath + "_single_response"
	err = ioutil.WriteFile(filename+".json", file, 0644)
	if err != nil {
		log.Error("Error writing to outfile : ", err)
	}
}

func main() {
	leaseHandler := leaseHandler{}

	// Load cmd params
	leaseHandler.getCmdParams()

	/*
		Initialize Logging
	*/
	err := PumiceDBCommon.InitLogger(leaseHandler.logFilePath)
	if err != nil {
		log.Error("Error while initializing the logger ", err)
	}

	// Start pmdbClient
	err = leaseHandler.startPMDBClient(leaseHandler.cliRequest.LeaseReq.Client.String())
	if err != nil {
		log.Error(err)
		os.Exit(-1)
	}

	leaseHandler.cliRequest.LeaseClientObj = &leaseHandler.clientObj
	switch leaseHandler.cliOperation {
	case leaseLib.GET:
		fallthrough
	case leaseLib.GET_VALIDATE:
		//get lease
		err = leaseHandler.getLeases()
	case leaseLib.LOOKUP:
		fallthrough
	case leaseLib.LOOKUP_VALIDATE:
		//lookup lease
		err = leaseHandler.lookupLeases()
	case leaseLib.REFRESH:
		// refresh lease
		err = leaseHandler.refreshLease()
	}

	if err != nil {
		log.Info("Operation failed")
	}
	log.Info("-----END OF EXECUTION-----")
}
