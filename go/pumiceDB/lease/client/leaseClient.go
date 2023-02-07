package leaseClient

import (
	"bytes"
	"common/leaseLib"
	"encoding/gob"
	"flag"
	"fmt"
	"os"
	"sync/atomic"

	pmdbClient "niova/go-pumicedb-lib/client"

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
