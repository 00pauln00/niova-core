package leaseClient

import (
	"bytes"
	"common/leaseLib"
	"encoding/gob"
	"errors"

	serviceDiscovery "common/clientAPI"
	pmdbClient "niova/go-pumicedb-lib/client"
	PumiceDBCommon "niova/go-pumicedb-lib/common"

	uuid "github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
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

type LeaseClient struct {
	RaftUUID            uuid.UUID
	PmdbClientObj       *pmdbClient.PmdbClientObj
	ServiceDiscoveryObj *serviceDiscovery.ServiceDiscoveryHandler
}

type LeaseClientReqHandler struct {
	Rncui          string
	LeaseClientObj *LeaseClient
	LeaseReq       leaseLib.LeaseReq
	LeaseRes       leaseLib.LeaseRes
	Err            error
}

func preparePumiceReq(leaseReq leaseLib.LeaseReq) []byte {
	var err error
	var pmdbReq PumiceDBCommon.PumiceRequest

	var leaseReqBuf bytes.Buffer
	leaseEnc := gob.NewEncoder(&leaseReqBuf)
	err = leaseEnc.Encode(leaseReq)
	if err != nil {
		log.Error(err)
		return nil
	}

	pmdbReq.Rncui = uuid.NewV4().String() + ":0:0:0:0"
	pmdbReq.ReqType = PumiceDBCommon.LEASE_REQ
	pmdbReq.ReqPayload = leaseReqBuf.Bytes()

	var pumiceReqBuf bytes.Buffer
	pumiceEnc := gob.NewEncoder(&pumiceReqBuf)
	err = pumiceEnc.Encode(pmdbReq)
	if err != nil {
		log.Error(err)
		return nil
	}
	return pumiceReqBuf.Bytes()
}

/*
Structure : LeaseHandler
Method	  : write()
Arguments : LeaseReq, rncui, *LeaseResp
Return(s) : error
Description : Wrapper function for WriteEncoded() function
*/
func (clientObj LeaseClient) write(reqBytes *[]byte, rncui string, response *[]byte) error {
	var err error
	var replySize int64

	reqArgs := &pmdbClient.PmdbReqArgs{
		Rncui:       rncui,
		ReqByteArr:  *reqBytes,
		GetResponse: 1,
		ReplySize:   &replySize,
		Response:    response,
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
func (clientObj LeaseClient) Read(reqBytes *[]byte, rncui string, response *[]byte) error {

	reqArgs := &pmdbClient.PmdbReqArgs{
		Rncui:      rncui,
		ReqByteArr: *reqBytes,
		Response:   response,
	}

	return clientObj.PmdbClientObj.ReadEncoded(reqArgs)
}

/*
Structure : LeaseHandler
Method	  : InitLeaseReq()
Arguments :
Return(s) : error
Description : Initialize the handler's leaseReq struct
*/
func (handler *LeaseClientReqHandler) InitLeaseReq(client, resource, rncui string, operation int) error {
	resourceUUID, err := uuid.FromString(resource)
	if err != nil {
		log.Error(err)
		return err
	}

	handler.LeaseReq.Operation = operation
	handler.LeaseReq.Resource = resourceUUID

	if operation != leaseLib.LOOKUP {
		clientUUID, err := uuid.FromString(client)
		if err != nil {
			log.Error(err)
			return err
		}
		handler.LeaseReq.Client = clientUUID
	}

	return err
}

/*
Structure : LeaseHandler
Method	  : Get()
Arguments :
Return(s) : error
Description : Handler function for get() operation
              Acquire a lease on a particular resource
*/
func (handler *LeaseClientReqHandler) Get() error {
	var err error
	var resBytes []byte

	// Prepare reqBytes for pumiceReq type
	reqBytes := preparePumiceReq(handler.LeaseReq)

	// send req
	err = handler.LeaseClientObj.write(&reqBytes, handler.Rncui, &resBytes)
	if err != nil {
		return err
	}

	// decode req response
	dec := gob.NewDecoder(bytes.NewBuffer(resBytes))
	err = dec.Decode(&handler.LeaseRes)
	if err != nil {
		return err
	}

	log.Info("Write request status - ", handler.LeaseRes.Status)

	return err
}

/*
Structure : LeaseHandler
Method	  : lookup()
Arguments : leaseLib.LeaseReq
Return(s) : error
Description : Handler function for lookup() operation
              Lookup lease info of a particular resource
*/
func (handler *LeaseClientReqHandler) Lookup() error {
	var err error
	var resBytes []byte

	// Prepare reqBytes for pumiceReq type
	reqBytes := preparePumiceReq(handler.LeaseReq)
	err = handler.LeaseClientObj.Read(&reqBytes, "", &resBytes)
	if err != nil {
		return err
	} else if len(resBytes) == 0 {
		err = errors.New("Key not found")
		return err
	}

	dec := gob.NewDecoder(bytes.NewBuffer(resBytes))
	err = dec.Decode(&handler.LeaseRes)
	if err != nil {
		return err
	}

	return err
}

/*
Structure : LeaseHandler
Method	  : refresh()
Arguments : leaseLib.LeaseReq
Return(s) : error
Description : Handler function for refresh() operation
              Refresh lease of a owned resource
*/
func (handler *LeaseClientReqHandler) Refresh() error {
	var err error
	var resBytes []byte

	// Prepare reqBytes for pumiceReq type
	reqBytes := preparePumiceReq(handler.LeaseReq)

	// send req
	err = handler.LeaseClientObj.write(&reqBytes, handler.Rncui, &resBytes)
	if err != nil {
		return err
	}

	// decode req response
	dec := gob.NewDecoder(bytes.NewBuffer(resBytes))
	err = dec.Decode(&handler.LeaseRes)
	if err != nil {
		return err
	}

	log.Info("Refresh request status - ", handler.LeaseRes.Status)

	return err
}

/*
Structure : LeaseHandler
Method	  : LeaseOperationOverHttp()
Arguments :
Return(s) : error
Description :
*/
func (handler *LeaseClientReqHandler) LeaseOperationOverHTTP() error {
	var err error
	var isWrite bool = false
	var resBytes []byte

	// Prepare reqBytes for pumiceReq type
	reqBytes := preparePumiceReq(handler.LeaseReq)

	if handler.LeaseReq.Operation != leaseLib.LOOKUP {
		isWrite = true
	}
	// send req
	resBytes, err = handler.LeaseClientObj.ServiceDiscoveryObj.Request(reqBytes, handler.Rncui, isWrite)
	//resBytes, err = clientObj.clientAPIObj.Request(reqBytes, rncui, true)
	if err != nil {
		return err
	}

	// decode the response if response is not blank
	if len(resBytes) == 0 {
		handler.LeaseRes.Status = leaseLib.FAILURE
		handler.Err = errors.New("Key not found")
	} else {
		handler.LeaseRes.Status = leaseLib.SUCCESS
		dec := gob.NewDecoder(bytes.NewBuffer(resBytes))
		err = dec.Decode(&handler.LeaseRes)
		if err != nil {
			return err
		}
	}
	log.Info("Lease request status - ", handler.LeaseRes.Status)

	return err
}
