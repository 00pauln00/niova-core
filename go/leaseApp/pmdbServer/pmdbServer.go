package main

import (
	"bytes"
	"common/requestResponseLib"
	"encoding/gob"
	"errors"
	"flag"
	"fmt"
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
	Major uint32
	Minor uint64
}

type leaseStruct struct {
	Resource     uuid.UUID
	Client       uuid.UUID
	Status       int
	LeaseGranted hybridTS
	LeaseExpiry  hybridTS
}

type leaseServer struct {
	raftUUID       string
	peerUUID       string
	logDir	       string
	logLevel       string
	leaseMap       map[uuid.UUID]leaseStruct
	pso            *PumiceDBServer.PmdbServerObject
}

func main() {
	lso, pErr := parseArgs()
	if pErr != nil {
		log.Println(pErr)
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

func provideLease(entry leaseStruct, clientUUID uuid.UUID) bool {
	//Check if existing lease is valid
	//If valid, Check if client uuid is same
	 //If so, return true
	 //If not, ret false
	//If not valid, ret true
	return true
}

func (lso *leaseServer) WritePrep(appId unsafe.Pointer, inputBuf unsafe.Pointer,
	inputBufSize int64, pmdbHande unsafe.Pointer) int {

	log.Trace("Lease server : Write prep request")

	Request := &requestResponseLib.LeaseReq{}

	decodeErr := lso.pso.Decode(inputBuf, Request, inputBufSize)
	if decodeErr != nil {
		log.Error("Failed to decode the application data")
		return -1
	}
	//Check if its a lease request
	//If not, return 0

	//Check if requested Vdev uuid is already present in MAP and has valid lease (1)
	//If so, check client UUID (2)
	//If matches (2), Return Status ok
	//If not(2), Return Error
	//If not(1), Create Map entry with status as mounting
	vdev_lease_info, isPresent := lso.leaseMap[Request.Resource]
	if isPresent {
	   if !provideLease(vdev_lease_info, Request.Client) {
		//Dont provide lease
		return -1
	   }
	}

	//Insert into MAP
	lso.leaseMap[Request.Resource] = leaseStruct{
		Resource: Request.Resource,
		Client: Request.Client,
		Status: MOUNTING,
	} 

	return 0
}

func (lso *leaseServer) Apply(appId unsafe.Pointer, inputBuf unsafe.Pointer,
	inputBufSize int64, pmdbHandle unsafe.Pointer) int {
	log.Trace("Lease server: Apply request received")
	var valueBytes bytes.Buffer

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

	leaseObj := leaseStruct{
		Resource: applyLeaseReq.Resource,
		Client:   applyLeaseReq.Client,
	}
	leaseObj.LeaseGranted.Major = 3
	leaseObj.LeaseGranted.Minor = 1066
	leaseObj.LeaseExpiry.Major = 3
	leaseObj.LeaseExpiry.Minor = 1166

	enc := gob.NewEncoder(&valueBytes)
	err := enc.Encode(&leaseObj)
	if err != nil {
		log.Error(err)
	}

	byteToStr := string(valueBytes.Bytes())
	log.Trace("Value passed by client: ", byteToStr)

	// Length of value.
	valLen := len(byteToStr)

	rc := lso.pso.WriteKV(appId, pmdbHandle, applyLeaseReq.Client.String(),
		int64(keyLength), byteToStr,
		int64(valLen), colmfamily)
	return rc
}

func (lso *leaseServer) Read(appId unsafe.Pointer, requestBuf unsafe.Pointer,
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
		inputVal, _ := uuid.Parse(string(valType))

		leaseObj := leaseStruct{}
		leaseObj.Status = 1
		dec := gob.NewDecoder(bytes.NewBuffer(readResult))
		err := dec.Decode(&leaseObj)
		if err != nil {
			log.Error(err)
		}
		fmt.Println("YYYY - ", leaseObj)
		log.Trace("Input value after read request:", inputVal)

		/*
			resultReq := requestResponseLib.LeaseReq{
				Client:   reqStruct.Client,
				Resource: inputVal,
			}
		*/

		//Copy the encoded result in replyBuffer
		replySize, copyErr = lso.pso.CopyDataToBuffer(leaseObj, replyBuf)
		if copyErr != nil {
			log.Error("Failed to Copy result in the buffer: %s", copyErr)
			return -1
		}
	} else {
		log.Error(readErr)
	}

	return replySize
}
