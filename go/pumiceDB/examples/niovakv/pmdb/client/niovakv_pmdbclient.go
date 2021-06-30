package niovakvpmdbclient

import (
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"niova/go-pumicedb-lib/client"
	"niovakv/niovakvlib"
	"os"
	"sync"
)

//Structure definition for client.
type NiovaKVClient struct {
	ClientObj  *PumiceDBClient.PmdbClientObj
	Rncui      string
	rncui_lock sync.Mutex
}

var numWReq, numRReq int

//Method for write operation.
func (nco *NiovaKVClient) Write(ReqObj *niovakvlib.NiovaKV) error {
	var errorMsg error
	//Perform write operation.
	nco.rncui_lock.Lock()
	rncui := fmt.Sprintf("%s:0:0:0:%d", nco.Rncui, numWReq)
	numWReq = numWReq + 1
	nco.rncui_lock.Unlock()
	err := nco.ClientObj.Write(ReqObj, rncui)
	if err != nil {
		log.Error("Write key-value failed : ", err)
		errorMsg = errors.New("Write operation failed.")
	} else {
		log.Info("Pmdb Write successful!")
		errorMsg = nil
	}
	return errorMsg
}

//Method to perform read operation.
func (nco *NiovaKVClient) Read(ReqObj *niovakvlib.NiovaKV) ([]byte, error) {

	rop := &niovakvlib.NiovaKV{}
	nco.rncui_lock.Lock()
	rncui := fmt.Sprintf("%s:0:0:0:%d", nco.Rncui, numRReq)
	numRReq = numRReq + 1
	nco.rncui_lock.Unlock()
	err := nco.ClientObj.Read(ReqObj, rncui, rop)
	if err != nil {
		log.Error("Read request failed !!", err)
	} else {
		log.Info("Result of the read request is:", rop)
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
		if err != nil {
			log.Error(err)
		} else {
			log.Info("Write operation successful")
		}

	case "read":
		value, err = nkvClient.Read(reqObj)
		if err != nil {
			log.Error(err)
		} else {
			log.Info("Data received after read request:", value)
		}

	case "exit":
		os.Exit(0)
	default:
		fmt.Print("\nEnter valid operation....")
	}
	return value, err
}
