package leaseClient

import (
	"bytes"
	"common/leaseLib"
	"encoding/gob"

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
		"GET":     leaseLib.GET,
		"PUT":     leaseLib.PUT,
		"LOOKUP":  leaseLib.LOOKUP,
		"REFRESH": leaseLib.REFRESH,
	}
)

type LeaseClient struct {
	RaftUUID      uuid.UUID
	PmdbClientObj *pmdbClient.PmdbClientObj
}

type LeaseClientReqHandler struct {
	Rncui          string
	LeaseClientObj *LeaseClient
	LeaseReq       leaseLib.LeaseReq
	LeaseRes       leaseLib.LeaseRes
	Err            error
}

func PrepareLeaseReq(client, resource string, operation int) []byte {
	var leaseReq leaseLib.LeaseReq
	var err error

	leaseReq.Client, err = uuid.FromString(client)
	leaseReq.Resource, err = uuid.FromString(resource)
	leaseReq.Operation = operation
	leaseReq.Rncui = uuid.NewV4().String() + ":0:0:0:0"
	if err != nil {
		log.Error(err)
		return nil
	}

	return PreparePumiceReq(leaseReq)
}

func PreparePumiceReq(leaseReq leaseLib.LeaseReq) []byte {
	var err error
	var pumiceReq PumiceDBCommon.PumiceRequest

	var leaseReqBuf bytes.Buffer
	leaseEnc := gob.NewEncoder(&leaseReqBuf)
	err = leaseEnc.Encode(leaseReq)
	if err != nil {
		log.Error(err)
		return nil
	}

	pumiceReq.Rncui = uuid.NewV4().String() + ":0:0:0:0"
	pumiceReq.ReqType = PumiceDBCommon.LEASE_REQ
	pumiceReq.ReqPayload = leaseReqBuf.Bytes()

	var pumiceReqBuf bytes.Buffer
	pumiceEnc := gob.NewEncoder(&pumiceReqBuf)
	err = pumiceEnc.Encode(pumiceReq)
	if err != nil {
		log.Error(err)
		return nil
	}
	return pumiceReqBuf.Bytes()
}
