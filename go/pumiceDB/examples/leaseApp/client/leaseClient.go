package main

import (
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
	cliReqArr    []leaseClientLib.LeaseClientReqHandler
	cliOperation int
	jsonFilePath string
	logFilePath  string
	numOfLeases  int
	readJsonFile string
	err          error
}

func usage() {
	flag.PrintDefaults()
	os.Exit(0)
}

func parseOperation(str string) (int, bool) {
	op, ok := operationsMap[str]
	return op, ok
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
	var tempReq leaseClientLib.LeaseClientReqHandler
	tempReq.LeaseReq.Client, err = uuid.FromString(strClientUUID)
	if err != nil {
		usage()
		os.Exit(-1)
	}
	if strResourceUUID != "" {
		tempReq.LeaseReq.Resource, err = uuid.FromString(strResourceUUID)
		if err != nil {
			usage()
			os.Exit(-1)
		}
	}
	handler.cliReqArr = append(handler.cliReqArr, tempReq)
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

	//Store rncui in AppUUID
	handler.clientObj.PmdbClientObj.AppUUID = uuid.NewV4().String()
	return nil
}

/*
Description : Fill up cliReqArr with N number of client and resource UUIDs
*/
func (lh *leaseHandler) prepReqs() {
	if lh.cliReqArr[0].LeaseReq.Resource != uuid.Nil {
		lh.numOfLeases = 1
		lh.cliReqArr[0].Rncui = getRNCUI(lh.clientObj.PmdbClientObj)
		lh.cliReqArr[0].LeaseClientObj = &lh.clientObj
		lh.cliReqArr[0].LeaseReq.Operation = lh.cliOperation
	} else {
		if lh.cliReqArr[0].LeaseReq.Operation == leaseLib.GET ||
			lh.cliOperation == leaseLib.GET_VALIDATE {
			for i := 0; i < lh.numOfLeases; i++ {
				lh.cliReqArr[i].InitLeaseReq(uuid.NewV4().String(), uuid.NewV4().String(), lh.cliOperation)
				lh.cliReqArr[i].Rncui = getRNCUI(lh.clientObj.PmdbClientObj)
				lh.cliReqArr[i].LeaseClientObj = &lh.clientObj
			}
		}
	}
}

func (lh *leaseHandler) validateCliReqArr() {
	mapString := make(map[string]string)

	for i := 0; i < len(lh.cliReqArr); i++ {
		if lh.cliReqArr[i].LeaseRes.Status == leaseLib.SUCCESS {
			mapString["Status"] = "Success"
		} else {
			log.Info("response status not matched")
			mapString["Status"] = "Failure"
			break
		}
	}
	lh.writeSingleResponseToJson(mapString)
}

/*
Structure : leaseHandler
Method	  : writeResToJson
Arguments : struct
Return(s) : error

Description : Write detailed request-response/error to json file
*/
func (lh *leaseHandler) writeResToJson() {
	b, err := json.MarshalIndent(lh.cliReqArr, "", " ")
	err = ioutil.WriteFile(lh.jsonFilePath+".json", b, 0644)
	lh.err = err
}

/*
Description : Write single response/error to json file
*/
func (lh *leaseHandler) writeSingleResponseToJson(toJson interface{}) {
	var filename string
	file, err := json.MarshalIndent(toJson, "", " ")
	filename = lh.jsonFilePath + "_single_response"
	err = ioutil.WriteFile(filename+".json", file, 0644)
	if err != nil {
		log.Error("Error writing to outfile : ", err)
	}
}

/*
Description : Perform lease operation for Get or Lookup
*/
func (lh *leaseHandler) performGetNLookup() error {
	for i := range lh.cliReqArr {
		// perform op
		if lh.cliOperation == leaseLib.GET || lh.cliOperation == leaseLib.GET_VALIDATE {
			lh.cliReqArr[i].Err = lh.cliReqArr[i].Get()
		} else {
			lh.cliReqArr[i].Err = lh.cliReqArr[i].Lookup()
		}
		// check err
		if lh.cliReqArr[i].Err != nil {
			//TODO Check if we should stop the loop if any one req is failed
			log.Error(lh.cliReqArr[i].Err)
			lh.err = lh.cliReqArr[i].Err
			lh.cliReqArr[i].LeaseRes.Status = leaseLib.FAILURE
		} else {
			lh.cliReqArr[i].LeaseRes.Status = leaseLib.SUCCESS
		}
	}
	// check if op is validate type
	if lh.cliOperation == leaseLib.GET_VALIDATE || lh.cliOperation == leaseLib.LOOKUP_VALIDATE {
		lh.validateCliReqArr()
	}
	lh.writeResToJson()

	return lh.err
}

/*
Description: Perform REFRESH lease operation
*/
func (lh *leaseHandler) refreshLease() error {
	rq := &lh.cliReqArr[0]

	rq.Err = rq.Refresh()
	if rq.Err != nil {
		log.Error(rq.Err)
		rq.LeaseRes.Status = leaseLib.FAILURE
	} else {
		rq.LeaseRes.Status = leaseLib.SUCCESS
	}
	// Write the response to the json file.
	lh.writeResToJson()
	return rq.Err
}

func main() {
	lh := leaseHandler{}

	// Load cmd params
	lh.getCmdParams()

	// Initialize Logging
	err := PumiceDBCommon.InitLogger(lh.logFilePath)
	if err != nil {
		log.Error("Error while initializing the logger ", err)
	}

	// Start pmdbClient
	err = lh.startPMDBClient(lh.cliReqArr[0].LeaseReq.Client.String())
	if err != nil {
		log.Error(err)
		os.Exit(-1)
	}

	lh.cliReqArr[0].LeaseClientObj = &lh.clientObj

	lh.prepReqs()
	if lh.cliOperation == leaseLib.REFRESH {
		err = lh.refreshLease()
	} else {
		err = lh.performGetNLookup()
	}
	if err != nil {
		log.Info("Operation failed")
	}
	log.Info("-----END OF EXECUTION-----")
}
