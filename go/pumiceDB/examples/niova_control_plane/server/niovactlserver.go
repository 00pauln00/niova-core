package main

import (
	"flag"
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"strconv"
	"unsafe"

	"niova/go-pumicedb-lib/server"
	"niovactlplane/lib"
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

	cso := parseArgs()

	//Create log directory if not Exist.
	makeDirectoryIfNotExists()

	//Create log file.
	initLogger(cso)

	log.Info("Raft UUID: %s", cso.raftUuid)
	log.Info("Peer UUID: %s", cso.peerUuid)

	/*
	   Initialize the internal pmdb-server-object pointer.
	   Assign the Directionary object to PmdbAPI so the apply and
	   read callback functions can be called through pmdb common library
	   functions.
	*/
	cso.pso = &PumiceDBServer.PmdbServerObject{
		ColumnFamilies: colmfamily,
		RaftUuid:       cso.raftUuid,
		PeerUuid:       cso.peerUuid,
		PmdbAPI:        cso,
	}

	// Start the pmdb server
	err := cso.pso.Run()

	if err != nil {
		log.Error(err)
	}
}

func parseArgs() *NiovaCtlServer {
	cso := &NiovaCtlServer{}

	flag.StringVar(&cso.raftUuid, "r", "NULL", "raft uuid")
	flag.StringVar(&cso.peerUuid, "u", "NULL", "peer uuid")
	flag.StringVar(&logDir, "l", "/tmp/niovaCtlPlane", "log dir")

	flag.Parse()

	return cso
}

/*If log directory is not exist it creates directory.
  and if dir path is not passed then it will create
  log file in "/tmp/niovaCtlPlane" path.
*/
func makeDirectoryIfNotExists() error {

	if _, err := os.Stat(logDir); os.IsNotExist(err) {

		return os.Mkdir(logDir, os.ModeDir|0755)
	}

	return nil
}

//Create logfile for each peer.
func initLogger(cso *NiovaCtlServer) {

	var filename string = logDir + "/" + cso.peerUuid + ".log"

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

type NiovaCtlServer struct {
	raftUuid       string
	peerUuid       string
	columnFamilies string
	pso            *PumiceDBServer.PmdbServerObject
}

func (cso *NiovaCtlServer) Apply(appId unsafe.Pointer, inputBuf unsafe.Pointer,
	inputBufSize int64, pmdbHandle unsafe.Pointer) {

	log.Info("NiovaCtlPlane server: Apply request received")

	// Decode the input buffer into structure format
	applyNiovaCtlReq := &niovareqlib.NiovaCtlReq{}

	decodeErr := cso.pso.Decode(inputBuf, applyNiovaCtlReq, inputBufSize)
	if decodeErr != nil {
		log.Error("Failed to decode the application data")
		return
	}

	log.Info("Key passed by client: ", applyNiovaCtlReq.InputKey)

	// length of key.
	keyLength := len(applyNiovaCtlReq.InputKey)

	// Lookup the key first
	prevResult, err := cso.pso.LookupKey(applyNiovaCtlReq.InputKey,
		int64(keyLength), colmfamily)

	var updatedVal int

	if err == nil {

		//Convert value []byte to string.
		byteToStrVal, _ := strconv.Atoi(string(applyNiovaCtlReq.InputValue))

		//Convert data type to int
		intVal, _ := strconv.Atoi(prevResult)
		//Update int type value.
		updatedVal = byteToStrVal + intVal
	}

	//Convert new updated value to string.
	strValue := strconv.Itoa(updatedVal)

	// Length of value.
	valLen := len(strValue)

	log.Info("Write the KeyValue by calling PmdbWriteKV")
	cso.pso.WriteKV(appId, pmdbHandle, applyNiovaCtlReq.InputKey,
		int64(keyLength), strValue,
		int64(valLen), colmfamily)

}

func (cso *NiovaCtlServer) Read(appId unsafe.Pointer, requestBuf unsafe.Pointer,
	requestBufSize int64, replyBuf unsafe.Pointer, replyBufSize int64) int64 {

	log.Info("NiovaCtlPlane server: Read request received")

	//Decode the request structure sent by client.
	reqStruct := &niovareqlib.NiovaCtlReq{}
	decodeErr := cso.pso.Decode(requestBuf, reqStruct, requestBufSize)

	if decodeErr != nil {
		log.Error("Failed to decode the read request")
		return -1
	}

	log.Info("Key passed by client: ", reqStruct.InputKey)

	keyLen := len(reqStruct.InputKey)
	log.Info("Key length: ", keyLen)

	//Pass the work as key to PmdbReadKV and get the value from pumicedb
	readResult, readErr := cso.pso.ReadKV(appId, reqStruct.InputKey,
		int64(keyLen), colmfamily)

	var valType []byte

	if readErr == nil {
		valType = []byte(readResult)
		log.Info("Input value after read request:", valType)
	}

	resultReq := niovareqlib.NiovaCtlReq{
		InputKey:   reqStruct.InputKey,
		InputValue: valType,
	}

	//Copy the encoded result in replyBuffer
	replySize, copyErr := cso.pso.CopyDataToBuffer(resultReq, replyBuf)
	if copyErr != nil {
		log.Error("Failed to Copy result in the buffer: %s", copyErr)
		return -1
	}

	log.Info("Reply size: ", replySize)

	return replySize
}
