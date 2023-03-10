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

type LeaseClient struct {
	RaftUUID      uuid.UUID
	PmdbClientObj *pmdbClient.PmdbClientObj
}

// will be used to write req and res to json file
type WriteObj struct {
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

func (clientObj LeaseClient) getRNCUI() string {
	idq := atomic.AddUint64(&clientObj.PmdbClientObj.WriteSeqNo, uint64(1))
	rncui := fmt.Sprintf("%s:0:0:0:%d", clientObj.PmdbClientObj.AppUUID, idq)
	return rncui
}

/*
Structure : LeaseHandler
Method	  : write()
Arguments : LeaseReq, rncui, *LeaseResp
Return(s) : error

Description : Wrapper function for WriteEncoded() function
*/

func (clientObj LeaseClient) write(requestObj leaseLib.LeaseReq, rncui string, response *[]byte) error {
	var err error
	var requestBytes bytes.Buffer
	var replySize int64

	enc := gob.NewEncoder(&requestBytes)
	err = enc.Encode(requestObj)
	if err != nil {
		return err
	}
	reqArgs := &pmdbClient.PmdbReqArgs{
		Rncui:       rncui,
		ReqByteArr:  requestBytes.Bytes(),
		GetResponse: 1,
		ReplySize:   &replySize,
		Response:    response,
		ReqType:     1,
	}

	err = clientObj.PmdbClientObj.WriteEncodedAndGetResponse(reqArgs)

	return err
}

/*
Structure : LeaseHandler
Method	  : read()
Arguments : LeaseReq, rncui, *response
Return(s) : error

Description : Wrapper function for ReadEncoded() function
*/
func (clientObj LeaseClient) Read(requestObj leaseLib.LeaseReq, rncui string, response *[]byte) error {
	var err error
	var requestBytes bytes.Buffer
	enc := gob.NewEncoder(&requestBytes)
	err = enc.Encode(requestObj)
	if err != nil {
		return err
	}
	reqArgs := &pmdbClient.PmdbReqArgs{
		Rncui:      rncui,
		ReqByteArr: requestBytes.Bytes(),
		Response:   response,
		ReqType:    1,
	}

	return clientObj.PmdbClientObj.ReadEncoded(reqArgs)
}

/*
Structure : LeaseHandler
Method	  : get()
Arguments : leaseLib.LeaseReq
Return(s) : error

Description : Handler function for get() operation
              Acquire a lease on a particular resource
*/
func (clientObj LeaseClient) Get(requestObj leaseLib.LeaseReq) (leaseLib.LeaseRes, error) {
	var err error
	var responseBytes []byte
	var responseObj leaseLib.LeaseRes

	rncui := clientObj.getRNCUI()

	err = clientObj.write(requestObj, rncui, &responseBytes)
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
func (clientObj LeaseClient) Lookup(requestObj leaseLib.LeaseReq) (leaseLib.LeaseRes, error) {
	var err error
	var responseBytes []byte
	var responseObj leaseLib.LeaseRes

	err = clientObj.Read(requestObj, "", &responseBytes)
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
func (clientObj LeaseClient) Refresh(requestObj leaseLib.LeaseReq) (leaseLib.LeaseRes, error) {
	var err error
	var responseBytes []byte
	var responseObj leaseLib.LeaseRes

	rncui := clientObj.getRNCUI()
	err = clientObj.write(requestObj, rncui, &responseBytes)
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
