package main

import (
	"common/requestResponseLib"
	"errors"
	"flag"
	log "github.com/sirupsen/logrus"
	PumiceDBCommon "niova/go-pumicedb-lib/common"
	PumiceDBServer "niova/go-pumicedb-lib/server"
	"os"
)

/*
#include <stdlib.h>
#include <string.h>
*/
import "C"

var seqno = 0

// Use the default column family
var colmfamily = "PMDBTS_CF"

type pmdbServerHandler struct {
	raftUUID string
	peerUUID string
	logDir   string
	logLevel string
}

func main() {
	serverHandler := pmdbServerHandler{}
	nso, pErr := serverHandler.parseArgs()
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

	log.Info("Raft and Peer UUID: ", nso.raftUuid, " ", nso.peerUuid)
	/*
	   Initialize the internal pmdb-server-object pointer.
	   Assign the Directionary object to PmdbAPI so the apply and
	   read callback functions can be called through pmdb common library
	   functions.
	*/
	nso.pso = &PumiceDBServer.PmdbServerObject{
		RaftUuid:       nso.raftUuid,
		PeerUuid:       nso.peerUuid,
		PmdbAPI:        nso,
		SyncWrites:     false,
		CoalescedWrite: true,
		ColumnFamilies: []string{colmfamily},
	}

	// Start the pmdb server
	err = nso.pso.Run()

	if err != nil {
		log.Error(err)
	}
}

func usage() {
	flag.PrintDefaults()
	os.Exit(0)
}

func (handler *pmdbServerHandler) parseArgs() (*NiovaKVServer, error) {

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

	nso := &NiovaKVServer{}
	nso.raftUuid = handler.raftUUID
	nso.peerUuid = handler.peerUUID

	if nso == nil {
		err = errors.New("Not able to parse the arguments")
	} else {
		err = nil
	}

	return nso, err
}

type NiovaKVServer struct {
	raftUuid       string
	peerUuid       string
	columnFamilies string
	pso            *PumiceDBServer.PmdbServerObject
}

func (nso *NiovaKVServer) WritePrep(wrPrepArgs *PumiceDBServer.PmdbCbArgs) int64 {
	return 0
}

func (nso *NiovaKVServer) InitPeer(initPeerArgs *PumiceDBServer.PmdbCbArgs) {
	return
}

func (nso *NiovaKVServer) CleanupPeer(cleanupPeerArgs *PumiceDBServer.PmdbCbArgs) {
	return
}

func (nso *NiovaKVServer) Apply(applyArgs *PumiceDBServer.PmdbCbArgs) int64 {

	log.Trace("NiovaCtlPlane server: Apply request received")

	// Decode the input buffer into structure format
	applyNiovaKV := &requestResponseLib.KVRequest{}

	decodeErr := nso.pso.Decode(applyArgs.ReqBuf, applyNiovaKV,
								applyArgs.ReqSize)
	if decodeErr != nil {
		log.Error("Failed to decode the application data")
		return -1
	}

	log.Trace("Key passed by client: ", applyNiovaKV.Key)

	// length of key.
	keyLength := len(applyNiovaKV.Key)

	byteToStr := string(applyNiovaKV.Value)

	// Length of value.
	valLen := len(byteToStr)

	log.Trace("Write the KeyValue by calling PmdbWriteKV")
	rc := nso.pso.WriteKV(applyArgs.UserID, applyArgs.PmdbHandler,
		applyNiovaKV.Key,
		int64(keyLength), byteToStr,
		int64(valLen), colmfamily)

	return int64(rc)
}

func (nso *NiovaKVServer) Read(readArgs *PumiceDBServer.PmdbCbArgs) int64 {

	log.Trace("NiovaCtlPlane server: Read request received")

	//Decode the request structure sent by client.
	reqStruct := &requestResponseLib.KVRequest{}
	decodeErr := nso.pso.Decode(readArgs.ReqBuf, reqStruct, readArgs.ReqSize)

	if decodeErr != nil {
		log.Error("Failed to decode the read request")
		return -1
	}

	log.Trace("Key passed by client: ", reqStruct.Key)

	keyLen := len(reqStruct.Key)
	log.Trace("Key length: ", keyLen)

	//Pass the work as key to PmdbReadKV and get the value from pumicedb
	readResult, readErr := nso.pso.ReadKV(readArgs.UserID, reqStruct.Key,
		int64(keyLen), colmfamily)
	var valType []byte
	var replySize int64
	var copyErr error

	if readErr == nil {
		valType = readResult
		inputVal := string(valType)
		log.Trace("Input value after read request:", inputVal)

		resultReq := requestResponseLib.KVRequest{
			Key:   reqStruct.Key,
			Value: valType,
		}

		//Copy the encoded result in replyBuffer
		replySize, copyErr = nso.pso.CopyDataToBuffer(resultReq,
													readArgs.ReplyBuf)
		if copyErr != nil {
			log.Error("Failed to Copy result in the buffer: %s", copyErr)
			return -1
		}
	} else {
		log.Error(readErr)
	}

	log.Trace("Reply size: ", replySize)

	return replySize
}
