package niovakvpmdbclient

import (
	"errors"
	"fmt"
	PumiceDBClient "niova/go-pumicedb-lib/client"
	"sync/atomic"

	"niovakv/niovakvlib"

	log "github.com/sirupsen/logrus"
)

//Structure definition for client.
type NiovaKVClient struct {
	ClientObj       *PumiceDBClient.PmdbClientObj
	AppUuid         string
	write_seqno     uint64
	write_Seqno_Ptr *uint64
}

//Method for write operation.
func (nco *NiovaKVClient) Write(ReqObj *niovakvlib.NiovaKV) error {

	//Perform write operation.
	idq := atomic.AddUint64(nco.write_Seqno_Ptr, uint64(1))
	rncui := fmt.Sprintf("%s:0:0:0:%d", nco.AppUuid, idq)

	log.Trace("(PMDB Client) Request for Write : ", ReqObj)
	err := nco.ClientObj.Write(ReqObj, rncui)
	if err != nil {
		log.Error("(PMDB Client) Write failed for key : ", ReqObj.InputValue, " ", err)
	} else {
		log.Trace("(PMDB Client) Write successful for key ", ReqObj.InputKey)
	}
	return err
}

//Method to perform read operation.
func (nco *NiovaKVClient) Read(ReqObj *niovakvlib.NiovaKV) ([]byte, error) {

	rop := &niovakvlib.NiovaKV{}

	log.Trace("(PMDB Client) Request for read : ", ReqObj)
	err := nco.ClientObj.Read(ReqObj, "", rop)
	if err != nil {
		log.Error("(PMDB Client) Read failed for key : ", ReqObj.InputValue, " ", err)
	} else {
		log.Trace("(PMDB Client) Read successful for key ", rop.InputValue)
	}
	return rop.InputValue, err
}

//Function to initialize pumicedb client object.
func GetNiovaKVClientObj(raftUuid, clientUuid, logFilepath string) *NiovaKVClient {

	//Create new client object.
	clientObj := PumiceDBClient.PmdbClientNew(raftUuid, clientUuid)
	if clientObj == nil {
		return nil
	}
	ncc := &NiovaKVClient{}
	ncc.ClientObj = clientObj
	ncc.write_Seqno_Ptr = &ncc.write_seqno
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
		log.Error("(PMDB Client) Recieved not supported operation")
		err = errors.New("Operation not supported")
	}
	return value, err
}
