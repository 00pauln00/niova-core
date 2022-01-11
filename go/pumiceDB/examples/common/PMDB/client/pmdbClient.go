package pmdbClient

import (
	//"errors"
	"fmt"
	PumiceClient "niova/go-pumicedb-lib/client"
	"sync/atomic"
	log "github.com/sirupsen/logrus"
)

//Structure definition for client.
type PMDBClientHandler struct {
	ClientObj       *PumiceClient.PmdbClientObj
	AppUuid         string
	writeSeqno      uint64
}

//Method for write operation.
func (handler *PMDBClientHandler) Write(request []byte) ([]byte,error) {
	//Perform write operation.
	idq := atomic.AddUint64(&handler.writeSeqno, uint64(1))
	rncui := fmt.Sprintf("%s:0:0:0:%d", handler.AppUuid, idq)

	err := handler.ClientObj.WriteEncoded(request, rncui)
	//var status int
	if err != nil {
		log.Error("(PMDB Client) Write failed : ", err)
		//status = -1
	} else {
		log.Info("(PMDB Client) Write successful")
	}
	var response []byte
	/*
	respone := requestResponseLib.KVResponse{
		Status : status
	}
	*/
	return response,err
}

//Method to perform read operation.
func (handler *PMDBClientHandler) Read(request []byte) ([]byte, error) {
	var response []byte
	err := handler.ClientObj.ReadEncoded(request, &response,"")
	if err != nil {
		log.Error("(PMDB Client) Read failed : ", err)
	} else {
		log.Trace("(PMDB Client) Read successful")
	}
	fmt.Println(string(response))
	return response, err
}

//Function to initialize pumicedb client object.
func Get_PMDBClient_Obj(raftUuid, clientUuid, logFilepath string) *PMDBClientHandler {
	//Create new client object.
	clientObj := PumiceClient.PmdbClientNew(raftUuid, clientUuid)
	if clientObj == nil {
		return nil
	}
	PMDBClientObj := &PMDBClientHandler{}
	PMDBClientObj.ClientObj = clientObj
	return PMDBClientObj
}

//Function to perform operations.
/*
func (handler *PMDBClient) ProcessRequest(request []byte, write bool) ([]byte, error) {
	var (
		value []byte
		err   error
	)
	if write {
		err = handler.Write(request)
	} else {
		value, err = handler.Read(reqObj)
	}
	return value, err
}
*/
