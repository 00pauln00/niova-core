package main

import (
	"errors"
	"flag"
	PumiceDBCommon "niova/go-pumicedb-lib/common"
	PumiceDBServer "niova/go-pumicedb-lib/server"
	niovakvlib "niovakv/lib"
	"os"
	"unsafe"

	log "github.com/sirupsen/logrus"
)

/*
#include <stdlib.h>
#include <string.h>
*/
import "C"

var seqno = 0
var raftUuid string
var peerUuid string
var logDir string

// Use the default column family
var colmfamily = "PMDBTS_CF"

func main() {

	nso, pErr := parseArgs()
	if pErr != nil {
		log.Println(pErr)
		return
	}

	flag.Usage = usage
	flag.Parse()

	if flag.NFlag() == 0 {
		usage()
		os.Exit(-1)
	}

	/* If log path is not provided, it will use Default log path.
	   default log path: /tmp/<peer-uuid>.log
	*/
	defaultLog := "/" + "tmp" + "/" + nso.peerUuid + ".log"
	flag.StringVar(&logDir, "NULL", defaultLog, "log dir")
	flag.Parse()
	//Create log file.
	err := PumiceDBCommon.InitLogger(logDir)
	if err != nil {
		log.Error("Error while initating logger ", err)
		os.Exit(1)
	}

	log.Info("Raft and Peer UUID: ", nso.raftUuid, " ", nso.peerUuid)

	/*
	   Initialize the internal pmdb-server-object pointer.
	   Assign the Directionary object to PmdbAPI so the apply and
	   read callback functions can be called through pmdb common library
	   functions.
	*/
	nso.pso = &PumiceDBServer.PmdbServerObject{
		ColumnFamilies: colmfamily,
		RaftUuid:       nso.raftUuid,
		PeerUuid:       nso.peerUuid,
		PmdbAPI:        nso,
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

func parseArgs() (*NiovaKVServer, error) {

	var err error

	nso := &NiovaKVServer{}

	flag.StringVar(&nso.raftUuid, "r", "NULL", "raft uuid")
	flag.StringVar(&nso.peerUuid, "u", "NULL", "peer uuid")
	flag.StringVar(&logDir, "l", "NULL", "log dir")

	flag.Parse()
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

func (nso *NiovaKVServer) Apply(appId unsafe.Pointer, inputBuf unsafe.Pointer,
	inputBufSize int64, pmdbHandle unsafe.Pointer) {

	log.Info("NiovaCtlPlane server: Apply request received")

	// Decode the input buffer into structure format
	applyNiovaKV := &niovakvlib.NiovaKV{}

	decodeErr := nso.pso.Decode(inputBuf, applyNiovaKV, inputBufSize)
	if decodeErr != nil {
		log.Error("Failed to decode the application data")
		return
	}

	log.Info("Key passed by client: ", applyNiovaKV.InputKey)

	// length of key.
	keyLength := len(applyNiovaKV.InputKey)

	byteToStr := string(applyNiovaKV.InputValue)

	// Length of value.
	valLen := len(byteToStr)

	log.Info("Write the KeyValue by calling PmdbWriteKV")
	nso.pso.WriteKV(appId, pmdbHandle, applyNiovaKV.InputKey,
		int64(keyLength), byteToStr,
		int64(valLen), colmfamily)

}

func (nso *NiovaKVServer) Read(appId unsafe.Pointer, requestBuf unsafe.Pointer,
	requestBufSize int64, replyBuf unsafe.Pointer, replyBufSize int64) int64 {

	log.Info("NiovaCtlPlane server: Read request received")

	//Decode the request structure sent by client.
	reqStruct := &niovakvlib.NiovaKV{}
	decodeErr := nso.pso.Decode(requestBuf, reqStruct, requestBufSize)

	if decodeErr != nil {
		log.Error("Failed to decode the read request")
		return -1
	}

	log.Info("Key passed by client: ", reqStruct.InputKey)

	keyLen := len(reqStruct.InputKey)
	log.Info("Key length: ", keyLen)

	//Pass the work as key to PmdbReadKV and get the value from pumicedb
	readResult, readErr := nso.pso.ReadKV(appId, reqStruct.InputKey,
		int64(keyLen), colmfamily)

	var valType []byte
	var replySize int64
	var copyErr error

	if readErr == nil {
		valType = readResult
		inputVal := string(valType)
		log.Info("Input value after read request:", inputVal)

		resultReq := niovakvlib.NiovaKV{
			InputKey:   reqStruct.InputKey,
			InputValue: valType,
		}

		//Copy the encoded result in replyBuffer
		replySize, copyErr = nso.pso.CopyDataToBuffer(resultReq, replyBuf)
		if copyErr != nil {
			log.Error("Failed to Copy result in the buffer: %s", copyErr)
			return -1
		}
	} else {
		log.Error(readErr)
	}

	log.Info("Reply size: ", replySize)

	return replySize
}
