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
	"strconv"
	"strings"
	"unsafe"

	uuid "github.com/google/uuid"
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
var ttlDefault = 120

// Use the default column family
var colmfamily = "PMDBTS_CF"

type leaseServer struct {
	raftUUID string
	peerUUID string
	logDir   string
	logLevel string
	leaseMap map[uuid.UUID]*requestResponseLib.LeaseStruct
	pso      *PumiceDBServer.PmdbServerObject
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

	lso.leaseMap = make(map[uuid.UUID]*requestResponseLib.LeaseStruct)

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

func addMinorToHybrid(current_time requestResponseLib.LeaderTS, add_minor int) int64 {
	//TODO: Validate this func
	return current_time.LeaderTime + int64(add_minor)
}

func getMinor(time float64) int {
	stringTime := fmt.Sprintf("%f", time)
	minorComponent, _ := strconv.Atoi(strings.Split(stringTime, ".")[1])
	return minorComponent
}

func isPermitted(entry *requestResponseLib.LeaseStruct, clientUUID uuid.UUID, currentTime requestResponseLib.LeaderTS, operation int) bool {
	if entry.LeaseState == requestResponseLib.INPROGRESS {
		return false
	}
	leaseExpiryTS := addMinorToHybrid(entry.TimeStamp, entry.TTL)

	//Check if existing lease is valid; by comparing only the minor
	stillValid := leaseExpiryTS >= currentTime.LeaderTime
	//If valid, Check if client uuid is same and operation is refresh
	if stillValid {
		if (operation == requestResponseLib.REFRESH) && (clientUUID == entry.Client) {
			return true
		} else {
			return false
		}
	}
	return true
}

func (lso *leaseServer) GetLeaderTimeStamp(ts *requestResponseLib.LeaderTS) int {
	var plts PumiceDBServer.PmdbLeaderTS
	rc := PumiceDBServer.PmdbGetLeaderTimeStamp(&plts)
	ts.LeaderTerm = plts.LeaderTerm
	ts.LeaderTime = plts.LeaderTime
	return rc
}

func (lso *leaseServer) WritePrep(appId unsafe.Pointer, inputBuf unsafe.Pointer,
	inputBufSize int64, replyBuf unsafe.Pointer, reply_buf_size int64, continue_wr unsafe.Pointer) int64 {

	var copyErr error
	var replySize int64

	Request := &requestResponseLib.LeaseReq{}

	decodeErr := lso.pso.Decode(inputBuf, Request, inputBufSize)
	if decodeErr != nil {
		log.Error("Failed to decode the application data")
		return -1
	}
	log.Info("(Write prep)Lease server : Write prep request", Request)

	//Get current hybrid time
	//TODO: GetCurrentHybridTime() def this in pumiceDBServer.go
	var currentTime requestResponseLib.LeaderTS
	lso.GetLeaderTimeStamp(&currentTime)

	//Check if its a refresh request
	if Request.Operation == requestResponseLib.REFRESH {
		//Copy the encoded result in replyBuffer
		_, copyErr = lso.pso.CopyDataToBuffer(byte(0), continue_wr)
		if copyErr != nil {
			log.Error("Failed to Copy result in the buffer: %s", copyErr)
			return -1
		}

		vdev_lease_info, isPresent := lso.leaseMap[Request.Resource]
		if isPresent {
			if isPermitted(vdev_lease_info, Request.Client, currentTime, Request.Operation) {
				//Refresh the lease
				lso.leaseMap[Request.Resource].TimeStamp = currentTime
				lso.leaseMap[Request.Resource].TTL = ttlDefault
				//Copy the encoded result in replyBuffer
				replySize, copyErr = lso.pso.CopyDataToBuffer(*lso.leaseMap[Request.Resource], replyBuf)
				if copyErr != nil {
					log.Error("Failed to Copy result in the buffer: %s", copyErr)
					return -1
				}
				return replySize
			}
		}
		return -1
	}

	//Check if get lease
	if Request.Operation == requestResponseLib.GET {
		vdev_lease_info, isPresent := lso.leaseMap[Request.Resource]
		log.Info("Get lease operation")
		if isPresent {
			if !isPermitted(vdev_lease_info, Request.Client, currentTime, Request.Operation) {
				//Dont provide lease
				lso.pso.CopyDataToBuffer(byte(0), continue_wr)
				return -1
			}
		}
		log.Info("Resource not present in map")
		//Insert or update into MAP
		lso.leaseMap[Request.Resource] = &requestResponseLib.LeaseStruct{
			Resource:   Request.Resource,
			Client:     Request.Client,
			LeaseState: requestResponseLib.INPROGRESS,
		}
	}

	log.Info("Map after write prep : ", lso.leaseMap)
	_, copyErr = lso.pso.CopyDataToBuffer(byte(1), continue_wr)
	if copyErr != nil {
		log.Error("Failed to Copy result in the buffer: %s", copyErr)
		return -1
	}
	return 0
}

func (lso *leaseServer) Apply(appId unsafe.Pointer, inputBuf unsafe.Pointer,
	inputBufSize int64, replyBuf unsafe.Pointer, maxreplySize int64, pmdbHandle unsafe.Pointer) int64 {
	var valueBytes bytes.Buffer
	var copyErr error
	var replySizeRc int64

	// Decode the input buffer into structure format
	applyLeaseReq := &requestResponseLib.LeaseReq{}

	decodeErr := lso.pso.Decode(inputBuf, applyLeaseReq, inputBufSize)
	if decodeErr != nil {
		log.Error("Failed to decode the application data")
		return -1
	}

	log.Info("(Apply) Lease request by client : ", applyLeaseReq.Client.String(), " for resource : ", applyLeaseReq.Resource.String())

	// length of key.
	keyLength := len(applyLeaseReq.Client.String())

	leaseObj, isPresent := lso.leaseMap[applyLeaseReq.Resource]
	if !isPresent {
		leaseObj = &requestResponseLib.LeaseStruct{
			Resource: applyLeaseReq.Resource,
			Client:   applyLeaseReq.Client,
		}
		lso.leaseMap[applyLeaseReq.Resource] = leaseObj
	}
	leaseObj.LeaseState = requestResponseLib.GRANTED
	isLeaderFlag := lso.GetLeaderTimeStamp(&leaseObj.TimeStamp)
	leaseObj.TTL = ttlDefault

	enc := gob.NewEncoder(&valueBytes)
	err := enc.Encode(&leaseObj)
	if err != nil {
		log.Error(err)
	}

	byteToStr := string(valueBytes.Bytes())
	log.Trace("Value passed by client: ", byteToStr)

	// Length of value.
	valLen := len(byteToStr)

	rc := lso.pso.WriteKV(appId, pmdbHandle, applyLeaseReq.Resource.String(),
		int64(keyLength), byteToStr,
		int64(valLen), colmfamily)

	if rc < 0 {
		log.Error("Value not written to rocksdb")
		return -1
	}

	//Copy the encoded result in replyBuffer
	replySizeRc = 0
	if isLeaderFlag == 0 && replyBuf != nil {
		log.Info("(Apply) lease obj ", leaseObj, *leaseObj)
		replySizeRc, copyErr = lso.pso.CopyDataToBuffer(leaseObj, replyBuf)
		if copyErr != nil {
			log.Error("Failed to Copy result in the buffer: %s", copyErr)
			return -1
		}
	}
	return replySizeRc
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

	log.Trace("Key passed by client: ", reqStruct.Resource)

	keyLen := len(reqStruct.Resource.String())

	//Update timestamp
	leaseObj, isPresent := lso.leaseMap[reqStruct.Resource]

	var readResult []byte
	var readErr error

	//Read from rocksDB only if its not present in MAP
	if !isPresent {
		//Pass the work as key to PmdbReadKV and get the value from pumicedb
		readResult, readErr = lso.pso.ReadKV(appId, reqStruct.Client.String(),
			int64(keyLen), colmfamily)
	}

	var valType []byte
	var replySize int64
	var copyErr error

	if isPresent {
		if leaseObj.LeaseState == requestResponseLib.INPROGRESS {
			return -1
		}

		oldTS := leaseObj.TimeStamp
		//Leader happens only in leader
		lso.GetLeaderTimeStamp(&leaseObj.TimeStamp)
		//TODO: Possible wrap around
		ttl := ttlDefault - int(leaseObj.TimeStamp.LeaderTime-oldTS.LeaderTime)
		if ttl < 0 {
			leaseObj.TTL = 0
			leaseObj.LeaseState = requestResponseLib.EXPIRED
		} else {
			leaseObj.TTL = ttl
		}
		replySize, copyErr = lso.pso.CopyDataToBuffer(*leaseObj, replyBuf)
		if copyErr != nil {
			log.Error("Failed to Copy result in the buffer: %s", copyErr)
			return -1
		}
	} else if readErr == nil {
		valType = readResult
		inputVal, _ := uuid.Parse(string(valType))

		leaseObjRocksDB := requestResponseLib.LeaseStruct{}
		dec := gob.NewDecoder(bytes.NewBuffer(readResult))
		err := dec.Decode(&leaseObjRocksDB)
		if err != nil {
			log.Error(err)
		}
		fmt.Println("YYYY - ", leaseObjRocksDB)
		log.Trace("Input value after read request:", inputVal)

		//Copy the encoded result in replyBuffer
		replySize, copyErr = lso.pso.CopyDataToBuffer(leaseObjRocksDB, replyBuf)
		if copyErr != nil {
			log.Error("Failed to Copy result in the buffer: %s", copyErr)
			return -1
		}
	} else {
		log.Error(readErr)
	}

	return replySize
}

func (lso *leaseServer) InitLeader() {
	for _, leaseObj := range lso.leaseMap {
		rc := lso.GetLeaderTimeStamp(&leaseObj.TimeStamp)
		if rc != 0 {
			log.Error("Unable to get timestamp (InitLeader)")
		}
		leaseObj.TTL = ttlDefault
	}
}
