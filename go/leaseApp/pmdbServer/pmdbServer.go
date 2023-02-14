package main

import (
	leaseServerLib "LeaseLib/leaseServer"
	leaseLib "common/leaseLib"
	"common/requestResponseLib"
	"errors"
	"flag"
	PumiceDBCommon "niova/go-pumicedb-lib/common"
	PumiceDBServer "niova/go-pumicedb-lib/server"
	"os"

	uuid "github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
)

/*
#cgo LDFLAGS: -lniova -lniova_raft -lniova_pumice -lrocksdb
#include <stdlib.h>
#include <string.h>
#include <raft/pumice_db.h>
#include <raft/raft_net.h>
#include <raft/pumice_db_client.h>
*/
import "C"

var seqno = 0
var ttlDefault = 60

// Use the default column family
var colmfamily []string = []string{"PMDBTS_CF"}

type leaseServer struct {
	raftUUID string
	peerUUID string
	logDir   string
	logLevel string
	leaseObj leaseServerLib.LeaseServerObject
	pso      *PumiceDBServer.PmdbServerObject
}

func main() {
	lso, pErr := parseArgs()
	if pErr != nil {
		log.Println("Invalid args - ", pErr)
		return
	}

	switch lso.logLevel {
	case "Info":
		log.SetLevel(log.InfoLevel)
	case "Trace":
		log.SetLevel(log.TraceLevel)
	}

	//Create log file
	err := PumiceDBCommon.InitLogger(lso.logDir)
	if err != nil {
		log.Error("Error while initating logger ", err)
		os.Exit(1)
	}

	if err != nil {
		log.Fatal("Error while initializing serf agent ", err)
	}

	log.Info("Raft and Peer UUID: ", lso.raftUUID, " ", lso.peerUUID)
	/*
	   Initialize the internal pmdb-server-object pointer.
	   Assign the Directionary object to PmdbAPI so the apply and
	   read callback functions can be called through pmdb common library
	   functions.
	*/
	lso.pso = &PumiceDBServer.PmdbServerObject{
		ColumnFamilies: colmfamily,
		RaftUuid:       lso.raftUUID,
		PeerUuid:       lso.peerUUID,
		PmdbAPI:        lso,
		SyncWrites:     false,
		CoalescedWrite: true,
	}

	//TODO: Fill all fields of LeaseServerObject
	lso.leaseObj = leaseServerLib.LeaseServerObject{}
	lso.leaseObj.Pso = lso.pso
	lso.leaseObj.LeaseMap = make(map[uuid.UUID]*leaseLib.LeaseStruct)

	// Start the pmdb server
	err = lso.pso.Run()
	if err != nil {
		log.Error(err)
	}
}

func usage() {
	flag.PrintDefaults()
	os.Exit(0)
}

func parseArgs() (*leaseServer, error) {

	var err error
	lso := &leaseServer{}

	flag.StringVar(&lso.raftUUID, "r", "NULL", "raft uuid")
	flag.StringVar(&lso.peerUUID, "u", "NULL", "peer uuid")

	/* If log path is not provided, it will use Default log path.
	   default log path: /tmp/<peer-uuid>.log
	*/
	defaultLog := "/" + "tmp" + "/" + lso.peerUUID + ".log"
	flag.StringVar(&lso.logDir, "l", defaultLog, "log dir")
	flag.StringVar(&lso.logLevel, "ll", "Info", "Log level")
	flag.Parse()

	if lso == nil {
		err = errors.New("Not able to parse the arguments")
	} else {
		err = nil
	}

	return lso, err
}

func (lso *leaseServer) WritePrep(wrPrepArgs *PumiceDBServer.PmdbCbArgs) int64 {

	var copyErr error
	var replySize int64

	Request := &requestResponseLib.LeaseReq{}

	decodeErr := lso.pso.Decode(wrPrepArgs.ReqBuf, Request, wrPrepArgs.ReqSize)
	if decodeErr != nil {
		log.Error("Failed to decode the application data")
		return -1
	}
	log.Info("(Write prep)Lease server : Write prep request", Request)

	var returnObj interface{}
	rc := lso.leaseObj.Prepare(Request, &returnObj)

	if rc <= 0 {
		//Dont continue write
		_, copyErr = lso.pso.CopyDataToBuffer(byte(0), wrPrepArgs.ContinueWr)
		if copyErr != nil {
			log.Error("Failed to Copy result in the buffer: %s", copyErr)
			return -1
		}
		if rc == 0 {
			replySize, copyErr = lso.pso.CopyDataToBuffer(returnObj, wrPrepArgs.ReplyBuf)
			if copyErr != nil {
				log.Error("Failed to Copy result in the buffer: %s", copyErr)
				return -1
			}
			return replySize
		}
		return -1
	} else {
		//Continue write
		_, copyErr = lso.pso.CopyDataToBuffer(byte(1), wrPrepArgs.ContinueWr)
		if copyErr != nil {
			log.Error("Failed to Copy result in the buffer: %s", copyErr)
			return -1
		}
		return 0
	}

}

func (lso *leaseServer) Apply(applyArgs *PumiceDBServer.PmdbCbArgs) int64 {
	//var valueBytes bytes.Buffer
	var copyErr error
	var replySizeRc int64

	// Decode the input buffer into structure format
	applyLeaseReq := &requestResponseLib.LeaseReq{}

	decodeErr := lso.pso.Decode(applyArgs.ReqBuf, applyLeaseReq, applyArgs.ReqSize)
	if decodeErr != nil {
		log.Error("Failed to decode the application data")
		return -1
	}

	log.Info("(Apply) Lease request by client : ", applyLeaseReq.Client.String(), " for resource : ", applyLeaseReq.Resource.String())

	// length of key.
	//keyLength := len(applyLeaseReq.Client.String())

	var returnObj interface{}
	rc := lso.leaseObj.ApplyLease(applyLeaseReq, &returnObj, applyArgs.UserID, applyArgs.PmdbHandler)
	//Copy the encoded result in replyBuffer
	replySizeRc = 0
	if rc == 0 && applyArgs.ReplyBuf != nil {
		replySizeRc, copyErr = lso.pso.CopyDataToBuffer(returnObj, applyArgs.ReplyBuf)
		if copyErr != nil {
			log.Error("Failed to Copy result in the buffer: %s", copyErr)
			return -1
		}
	} else {
		return int64(rc)
	}

	return replySizeRc
}

func (lso *leaseServer) Read(readArgs *PumiceDBServer.PmdbCbArgs) int64 {

	log.Trace("NiovaCtlPlane server: Read request received")

	//Decode the request structure sent by client.
	reqStruct := &requestResponseLib.LeaseReq{}
	decodeErr := lso.pso.Decode(readArgs.ReqBuf, reqStruct, readArgs.ReqSize)

	if decodeErr != nil {
		log.Error("Failed to decode the read request")
		return -1
	}

	log.Trace("Key passed by client: ", reqStruct.Resource)

	//keyLen := len(reqStruct.Resource.String())

	var returnObj interface{}
	var replySize int64
	var copyErr error

	rc := lso.leaseObj.ReadLease(reqStruct.Resource, &returnObj)

	if rc == 0 {
		replySize, copyErr = lso.pso.CopyDataToBuffer(returnObj, readArgs.ReplyBuf)
		if copyErr != nil {
			log.Error("Failed to Copy result in the buffer: %s", copyErr)
			return -1
		}

		return replySize
	}

	return int64(rc)

}

func (lso *leaseServer) Init(initPeerArgs *PumiceDBServer.PmdbCbArgs) {
	if len(lso.leaseObj.LeaseMap) != 0 {
		lso.leaseObj.LeaderInit()
	} else {
		//lso.leaseObj.PeerBootup(initPeerArgs.UserID)
	}

}

func (lso *leaseServer) PrepPeer(prepPeer *PumiceDBServer.PmdbCbArgs) {
	return
}

func (lso *leaseServer) CleanupPeer(cleanupPeerArgs *PumiceDBServer.PmdbCbArgs) {
	return
}
