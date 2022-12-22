package main

import (
	"common/requestResponseLib"
	"errors"
	"flag"
	PumiceDBCommon "niova/go-pumicedb-lib/common"
	PumiceDBServer "niova/go-pumicedb-lib/server"
	"os"
	"unsafe"

	uuid "github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

/*
#include <stdlib.h>
#include <string.h>
*/
import "C"

var seqno = 0

// Use the default column family
var colmfamily = "PMDBTS_CF"

const (
	MOUNTING int = 0
	MOUNTED      = 1
	EXPIRED      = 2
	REVOKED      = 3
)

type hybridTS struct {
	major uint32
	minor uint64
}

type leaseStruct struct {
	vdevUUID     uuid.UUID
	clientUUID   uuid.UUID
	status       int
	leaseGranted hybridTS
	leaseExpiry  hybridTS
}

type pmdbServerHandler struct {
	raftUUID string
	peerUUID string
	logDir   string
	logLevel string
	pso      *PumiceDBServer.PmdbServerObject
}

type LeaseServer struct {
	raftUuid       string
	peerUuid       string
	columnFamilies string
	leaseMap       map[uuid.UUID]leaseStruct
	pso            *PumiceDBServer.PmdbServerObject
}

func main() {
	serverHandler := pmdbServerHandler{}
	lso, pErr := serverHandler.parseArgs()
	if pErr != nil {
		log.Println(pErr)
		return
	}

	switch serverHandler.logLevel {
	case "Info":
		log.SetLevel(log.InfoLevel)
	case "Trace":
		log.SetLevel(log.TraceLevel)
	}

	//Create log file
	err := PumiceDBCommon.InitLogger(serverHandler.logDir)
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

func (handler *pmdbServerHandler) parseArgs() (*pmdbServerHandler, error) {

	var err error

	flag.StringVar(&handler.raftUUID, "r", "NULL", "raft uuid")
	flag.StringVar(&handler.peerUUID, "u", "NULL", "peer uuid")

	/* If log path is not provided, it will use Default log path.
	   default log path: /tmp/<peer-uuid>.log
	*/
	defaultLog := "/" + "tmp" + "/" + handler.peerUUID + ".log"
	flag.StringVar(&handler.logDir, "l", defaultLog, "log dir")
	flag.StringVar(&handler.logLevel, "ll", "Info", "Log level")
	flag.Parse()

	lso := &pmdbServerHandler{}
	lso.raftUUID = handler.raftUUID
	lso.peerUUID = handler.peerUUID

	if lso == nil {
		err = errors.New("Not able to parse the arguments")
	} else {
		err = nil
	}

	return lso, err
}

func (lso *pmdbServerHandler) WritePrep(appId unsafe.Pointer, inputBuf unsafe.Pointer,
	inputBufSize int64, pmdbHande unsafe.Pointer) int {

	log.Trace("Lease server : Write prep request")

	Request := &requestResponseLib.LeaseReq{}

	decodeErr := lso.pso.Decode(inputBuf, Request, inputBufSize)
	if decodeErr != nil {
		log.Error("Failed to decode the application data")
		return -1
	}

	//Check if requested Vdev uuid is already present in MAP and has valid lease (1)
	//If so, check client UUID (2)
	//If matches (2), Return Status ok
	//If not(2), Return Error
	//If not(1), Create Map entry with status as mounting

	return 0
}

func (lso *pmdbServerHandler) Apply(appId unsafe.Pointer, inputBuf unsafe.Pointer,
	inputBufSize int64, pmdbHandle unsafe.Pointer) int {

	log.Trace("Lease server: Apply request received")

	// Decode the input buffer into structure format
	applyLeaseReq := &requestResponseLib.LeaseReq{}

	decodeErr := lso.pso.Decode(inputBuf, applyLeaseReq, inputBufSize)
	if decodeErr != nil {
		log.Error("Failed to decode the application data")
		return -1
	}

	log.Trace("Key passed by client: ", applyLeaseReq.Client.String())

	// length of key.
	keyLength := len(applyLeaseReq.Client.String())

	byteToStr := applyLeaseReq.Resource.String()
	log.Trace("Value passed by client: ", byteToStr)

	// Length of value.
	valLen := len(byteToStr)

	rc := lso.pso.WriteKV(appId, pmdbHandle, applyLeaseReq.Client.String(),
		int64(keyLength), byteToStr,
		int64(valLen), colmfamily)
	return rc
}

func (lso *pmdbServerHandler) Read(appId unsafe.Pointer, requestBuf unsafe.Pointer,
	requestBufSize int64, replyBuf unsafe.Pointer, replyBufSize int64) int64 {

	log.Trace("NiovaCtlPlane server: Read request received")

	//Decode the request structure sent by client.
	reqStruct := &requestResponseLib.LeaseReq{}
	decodeErr := lso.pso.Decode(requestBuf, reqStruct, requestBufSize)

	if decodeErr != nil {
		log.Error("Failed to decode the read request")
		return -1
	}

	log.Trace("Key passed by client: ", reqStruct.Client)

	keyLen := len(reqStruct.Client.String())

	//Pass the work as key to PmdbReadKV and get the value from pumicedb
	readResult, readErr := lso.pso.ReadKV(appId, reqStruct.Client.String(),
		int64(keyLen), colmfamily)
	var valType []byte
	var replySize int64
	var copyErr error

	if readErr == nil {
		valType = readResult
		inputVal := string(valType)
		log.Trace("Input value after read request:", inputVal)

		resultReq := requestResponseLib.LeaseReq{
			Client:   reqStruct.Client,
			Resource: reqStruct.Resource,
		}

		//Copy the encoded result in replyBuffer
		replySize, copyErr = lso.pso.CopyDataToBuffer(resultReq, replyBuf)
		if copyErr != nil {
			log.Error("Failed to Copy result in the buffer: %s", copyErr)
			return -1
		}
	} else {
		log.Error(readErr)
	}

	return replySize
}
