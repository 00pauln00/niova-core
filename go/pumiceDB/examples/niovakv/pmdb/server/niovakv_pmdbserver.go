package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"unsafe"

	log "github.com/sirupsen/logrus"

	"niova/go-pumicedb-lib/server"
	"niovakv/lib"
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

	//Print help message.
	if len(os.Args) == 1 || os.Args[1] == "-help" || os.Args[1] == "-h" {
		fmt.Println("Positional Arguments: \n		'-r' - RAFT UUID \n		'-u' - PEER UUID")
		fmt.Println("Optional Arguments: \n		'-l' - Log Dir Path \n		-h, -help")
		fmt.Println("niovactlserver -r <RAFT UUID> -u <PEER UUID> -l <log directory>")
		os.Exit(0)
	}

	nso := parseArgs()

	//If log path is not provided, it will use Default log path.
	defaultLog := "/" + "tmp" + "/" + nso.peerUuid + ".log"
	flag.StringVar(&logDir, "NULL", defaultLog, "log dir")
	flag.Parse()

	//Create log file.
	initLogger()

	log.Info("Raft UUID: ", nso.raftUuid)
	log.Info("Peer UUID: ", nso.peerUuid)

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
	err := nso.pso.Run()

	if err != nil {
		log.Error(err)
	}
}

func parseArgs() *NiovaKVServer {

	nso := &NiovaKVServer{}

	flag.StringVar(&nso.raftUuid, "r", "NULL", "raft uuid")
	flag.StringVar(&nso.peerUuid, "u", "NULL", "peer uuid")
	flag.StringVar(&logDir, "l", "NULL", "log dir")

	flag.Parse()

	return nso
}

//Create logfile for each peer.
func initLogger() {

	// Split log path
	parts := strings.Split(logDir, "/")
	fname := parts[len(parts)-1]
	dir := strings.TrimSuffix(logDir, fname)

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.MkdirAll(dir, 0700) // Create directory
	}

	filename := dir + fname
	fmt.Println("logfile:", filename)

	//Create the log file if doesn't exist. And append to it if it already exists.
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	Formatter := new(log.TextFormatter)

	//Set Timestamp format for logfile.
	Formatter.TimestampFormat = "02-01-2006 15:04:05"
	Formatter.FullTimestamp = true
	log.SetFormatter(Formatter)

	if err != nil {
		// Cannot open log file. Logging to stderr
		fmt.Println(err)
	} else {
		log.SetOutput(f)
	}
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

	if readErr == nil {
		valType = []byte(readResult)
		inputVal := string(valType)
		log.Info("Input value after read request:", inputVal)
	}

	resultReq := niovakvlib.NiovaKV{
		InputKey:   reqStruct.InputKey,
		InputValue: valType,
	}

	//Copy the encoded result in replyBuffer
	replySize, copyErr := nso.pso.CopyDataToBuffer(resultReq, replyBuf)
	if copyErr != nil {
		log.Error("Failed to Copy result in the buffer: %s", copyErr)
		return -1
	}

	log.Info("Reply size: ", replySize)

	return replySize
}
