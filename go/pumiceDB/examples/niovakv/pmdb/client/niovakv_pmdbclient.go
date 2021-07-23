package niovakvpmdbclient

import (
	"errors"
	"fmt"
	PumiceDBClient "niova/go-pumicedb-lib/client"
	"niovakv/niovakvlib"
	"sync"

	log "github.com/sirupsen/logrus"
)

//Structure definition for client.
type NiovaKVClient struct {
	ClientObj  *PumiceDBClient.PmdbClientObj
	AppUuid    string
	rncui_lock sync.Mutex
}

var numWReq int

//Method for write operation.
func (nco *NiovaKVClient) Write(ReqObj *niovakvlib.NiovaKV) error {
	var errorMsg error
	//Perform write operation.
	nco.rncui_lock.Lock()
	numWReq = numWReq + 1
	nco.rncui_lock.Unlock()
	rncui := fmt.Sprintf("%s:0:0:0:%d", nco.AppUuid, numWReq)
	err := nco.ClientObj.Write(ReqObj, rncui)
	if err != nil {
		log.Error("Pmdb Write failed for key ", ReqObj.InputKey)
		errorMsg = errors.New("Write operation failed")
	} else {
		log.Info("Pmdb Write successful for key ", ReqObj.InputKey)
		errorMsg = nil
	}
	return errorMsg
}

//Method to perform read operation.
func (nco *NiovaKVClient) Read(ReqObj *niovakvlib.NiovaKV) ([]byte, error) {

	rop := &niovakvlib.NiovaKV{}
	rncui := fmt.Sprintf("%s:0:0:0:0", nco.AppUuid)
	log.Info("rncui and req obj is :", rncui, ReqObj)
	nco.rncui_lock.Lock()
	err := nco.ClientObj.Read(ReqObj, rncui, rop)
	nco.rncui_lock.Unlock()
	if err != nil {
		log.Error("Read request failed for key with err ", ReqObj.InputKey, " ", err)
	} else {
		log.Info("Result of the read request for key is :", ReqObj.InputKey, " ", rop)
	}
	return rop.InputValue, err
}

//Function to get pumicedb client object.
func GetNiovaKVClientObj(raftUuid, clientUuid, logFilepath string) *NiovaKVClient {

	//Create new client object.
	clientObj := PumiceDBClient.PmdbClientNew(raftUuid, clientUuid)
	if clientObj == nil {
		return nil
	}
	ncc := &NiovaKVClient{}
	ncc.ClientObj = clientObj
	return ncc
}

//Function to perform operations.
func (nkvClient *NiovaKVClient) ProcessRequest(reqObj *niovakvlib.NiovaKV) ([]byte, error) {

	var (
		value []byte
		err   error
	)

	ops := reqObj.InputOps
	switch ops {
	case "write":
		err = nkvClient.Write(reqObj)
	case "read":
		value, err = nkvClient.Read(reqObj)
	default:
		err = errors.New("Operation not supported")
	}
	return value, err
}
