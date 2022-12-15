package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	pmdbClient "niova/go-pumicedb-lib/client"

	"github.com/google/uuid"
)

type leaseHandler struct {
	clientUUID    uuid.UUID
	vdevUUID      uuid.UUID
	raftUUID      uuid.UUID
	ttl           time.Duration
	pmdbClientObj *pmdbClient.PmdbClientObj
	operation     string
	state         int
	timeStamp     string
}

func usage() {
	flag.PrintDefaults()
	os.Exit(0)
}

/*
Structure : leaseHandler
Method	  : getCmdParams
Arguments : None
Return(s) : None

Description : Parse command line params and load into leaseHandler sturct
*/
func (handler *leaseHandler) getCmdParams() {
	var tempClientUUID, tempVdevUUID, tempRaftUUID string
	var err error

	flag.StringVar(&tempClientUUID, "u", uuid.New().String(), "ClientUUID - UUID of the requesting client")
	flag.StringVar(&tempVdevUUID, "v", "NULL", "VdevUUID - UUID of the requested VDEV")
	flag.StringVar(&tempRaftUUID, "ru", "NULL", "RaftUUID - UUID of the raft cluster")
	flag.StringVar(&handler.operation, "o", "2", "Operation - 0 : Write Lease Req, 1 : Read Lease Req, 2 : Refresh Lease Req")
	flag.Usage = usage
	flag.Parse()
	if flag.NFlag() == 0 {
		usage()
		os.Exit(-1)
	}

	handler.clientUUID, err = uuid.Parse(tempClientUUID)
	if err != nil {
		usage()
		os.Exit(-1)
	}
	handler.vdevUUID, err = uuid.Parse(tempVdevUUID)
	if err != nil {
		usage()
		os.Exit(-1)
	}
}

/*
Structure : leaseHandler
Method	  : startPMDBClient
Arguments : None
Return(s) : error

Description : Start PMDB Client object for ClientUUID and RaftUUID
*/
func (handler *leaseHandler) startPMDBClient() error {
	var err error

	//Get clientObj
	handler.pmdbClientObj = pmdbClient.PmdbClientNew(handler.raftUUID.String(), handler.clientUUID.String())
	if handler.pmdbClientObj == nil {
		return errors.New("PMDB Client Obj could not be initialized")
	}

	//Start PMDB Client
	err = handler.pmdbClientObj.Start()
	if err != nil {
		return err
	}

	//Store encui in AppUUID
	handler.pmdbClientObj.AppUUID = uuid.New().String()
	return nil
}

func main() {
	leaseObj := leaseHandler{}

	// Load cmd params
	leaseObj.getCmdParams()

	/*
		Initialize Logging
	*/

	err := leaseObj.startPMDBClient()
	if err != nil {
		fmt.Println(err)
	}

}
