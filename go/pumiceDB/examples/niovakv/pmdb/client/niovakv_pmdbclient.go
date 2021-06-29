package niovakvpmdbclient

import (
	"errors"
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"

	"niova/go-pumicedb-lib/client"
	"niovakv/niovakvlib"
)

//Structure definition for client.
type NiovaKVClient struct {
	ClientObj *PumiceDBClient.PmdbClientObj
	ReqObj    *niovakvlib.NiovaKV
	Rncui     string
}

var numReq int

//Method to perform write operation.
func (nco *NiovaKVClient) Write() error {

	var errorMsg error
	//Perform write operation.
	rncui := fmt.Sprintf("%s:0:0:0:%d", nco.Rncui, numReq)
	err := nco.ClientObj.Write(nco.ReqObj, rncui)
	if err != nil {
		log.Error("Write key-value failed : ", err)
		errorMsg = errors.New("Write operation failed.")
	} else {
		log.Info("Pmdb Write successful!")
		errorMsg = nil
	}
	numReq = numReq + 1
	return errorMsg
}

//Method to perform read operation.
func (nco *NiovaKVClient) Read() ([]byte, error) {

	var rErr error
	rop := &niovakvlib.NiovaKV{}
	rncui := fmt.Sprintf("%s:0:0:0:0", nco.Rncui)
	err := nco.ClientObj.Read(nco.ReqObj, rncui, rop)
	if err != nil {
		log.Error("Read request failed !!", err)
		rErr = errors.New("Read operation failed")
	} else {
		log.Info("Result of the read request is:", rop)
	}
	return rop.InputValue, rErr
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
func (nkvClient *NiovaKVClient) ProcessRequest() ([]byte, int) {

	var (
		value  []byte
		status int
	)

	ops := nkvClient.ReqObj.InputOps
	switch ops {

	case "write":
		err := nkvClient.Write()
		if err != nil {
			log.Error(err)
			status = -1
		} else {
			log.Info("Write operation successful")
			status = 0
		}

	case "read":
		rval, err := nkvClient.Read()
		if err != nil {
			log.Error(err)
			status = -1
		} else {
			value = rval
			log.Info("Data received after read request:", value)
		}

	case "exit":
		os.Exit(0)
	default:
		fmt.Print("\nEnter valid operation....")
	}

	return value, status
}
