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
	ReqBuff        bytes.Buffer
	Err            error
}

func (lh *LeaseClientReqHandler) preparePumiceReq() error {
	var err error
	var pmdbReq PumiceDBCommon.PumiceRequest

	var lrb bytes.Buffer
	lEnc := gob.NewEncoder(&lrb)
	err = lEnc.Encode(lh.LeaseReq)
	if err != nil {
		return err
	}

	pmdbReq.Rncui = uuid.NewV4().String() + ":0:0:0:0"
	pmdbReq.ReqType = PumiceDBCommon.LEASE_REQ
	pmdbReq.ReqPayload = lrb.Bytes()

	pEnc := gob.NewEncoder(&lh.ReqBuff)
	return pEnc.Encode(pmdbReq)
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
func (clientObj LeaseClient) read(reqBytes *[]byte, rncui string, response *[]byte) error {

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
func (lh *LeaseClientReqHandler) InitLeaseReq(client, resource, rncui string, operation int) error {
	rUUID, err := uuid.FromString(resource)
	if err != nil {
		log.Error(err)
		return err
	}
	cUUID, err := uuid.FromString(client)
	if err != nil {
		log.Error(err)
		return err
	}

	if operation == leaseLib.GET_VALIDATE {
		lh.LeaseReq.Operation = leaseLib.GET
	} else {
		lh.LeaseReq.Operation = operation
	}
	lh.LeaseReq.Resource = rUUID
	lh.LeaseReq.Client = cUUID
	lh.Rncui = rncui

	return err
}

/*
Structure : LeaseHandler
Method	  : LeaseOperation()
Arguments : leaseLib.LeaseReq
Return(s) : error
Description : Handler function for all lease operations
*/
func (lh *LeaseClientReqHandler) LeaseOperation() error {
	var err error
	var b []byte

	// Prepare reqBytes for pumiceReq type
	err = lh.preparePumiceReq()
	if err != nil {
		return err
	}

	// send req
	rqb := lh.ReqBuff.Bytes()
	switch lh.LeaseReq.Operation {
	case leaseLib.GET, leaseLib.GET_VALIDATE:
		fallthrough
	case leaseLib.REFRESH:
		err = lh.LeaseClientObj.write(&rqb, lh.Rncui, &b)
	case leaseLib.LOOKUP, leaseLib.LOOKUP_VALIDATE:
		err = lh.LeaseClientObj.read(&rqb, "", &b)
	}
	if err != nil {
		return err
	}

	// decode req response
	dec := gob.NewDecoder(bytes.NewBuffer(b))
	err = dec.Decode(&lh.LeaseRes)
	if err != nil {
		return err
	}

	log.Info("Lease request status - ", lh.LeaseRes.Status)

	return err
}

/*
Structure : LeaseHandler
Method	  : LeaseOperationOverHttp()
Arguments :
Return(s) : error
Description :
*/
func (lh *LeaseClientReqHandler) LeaseOperationOverHTTP() error {
	var err error
	var isWrite bool = false
	var b []byte

	// Prepare reqBytes for pumiceReq type
	err = lh.preparePumiceReq()
	if err != nil {
		return err
	}

	if lh.LeaseReq.Operation != leaseLib.LOOKUP {
		isWrite = true
	}
	// send req
	b, err = lh.LeaseClientObj.ServiceDiscoveryObj.Request(lh.ReqBuff.Bytes(), lh.Rncui, isWrite)
	if err != nil {
		return err
	}

	// decode the response if response is not blank
	if len(b) == 0 {
		lh.LeaseRes.Status = leaseLib.FAILURE
		lh.Err = errors.New("Key not found")
	} else {
		lh.LeaseRes.Status = leaseLib.SUCCESS
		dec := gob.NewDecoder(bytes.NewBuffer(b))
		err = dec.Decode(&lh.LeaseRes)
	}
	log.Info("Lease request status - ", lh.LeaseRes.Status)

	return err
}
