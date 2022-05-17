package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"unsafe"

	"covidapplib/lib"
	log "github.com/sirupsen/logrus"
	"niova/go-pumicedb-lib/server"
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
		fmt.Println("covid_app_server -r <RAFT UUID> -u <PEER UUID> -l <log directory>")
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

func parseArgs() *CovidServer {
	cso := &CovidServer{}

	flag.StringVar(&cso.raftUuid, "r", "NULL", "raft uuid")
	flag.StringVar(&cso.peerUuid, "u", "NULL", "peer uuid")
	flag.StringVar(&logDir, "l", "/tmp/covidAppLog", "log dir")

	flag.Parse()

	return cso
}

/*If log directory is not exist it creates directory.
  and if dir path is not passed then it will create
  log file in "/tmp/covidAppLog" path.
*/
func makeDirectoryIfNotExists() error {

	if _, err := os.Stat(logDir); os.IsNotExist(err) {

		return os.Mkdir(logDir, os.ModeDir|0755)
	}

	return nil
}

//Create logfile for each peer.
func initLogger(cso *CovidServer) {

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

type CovidServer struct {
	raftUuid       string
	peerUuid       string
	columnFamilies string
	pso            *PumiceDBServer.PmdbServerObject
}

func (cso *CovidServer) Apply(appId unsafe.Pointer, inputBuf unsafe.Pointer,
	inputBufSize int64, pmdbHandle unsafe.Pointer) int {

	log.Info("Covid19_Data app server: Apply request received")

	/* Decode the input buffer into structure format */
	applyCovid := &CovidAppLib.CovidLocale{}

	decodeErr := cso.pso.Decode(inputBuf, applyCovid, inputBufSize)
	if decodeErr != nil {
		log.Error("Failed to decode the application data")
		return -1
	}

	log.Info("Key passed by client: ", applyCovid.Location)

	//length of key.
	keyLength := len(applyCovid.Location)

	//Lookup the key first
	prevResult, err := cso.pso.LookupKey(applyCovid.Location,
		int64(keyLength), colmfamily)

	log.Info("Previous values of the covidData: ", prevResult)

	if err == nil {

		//Get TotalVaccinations value and PeopleVaccinated value by splitting prevResult.
		splitVal := strings.Split(string(prevResult), " ")

		//Convert data type to int64.
		tvInt, _ := strconv.ParseInt(splitVal[len(splitVal)-2], 10, 64)
		//update TotalVaccinations.
		applyCovid.TotalVaccinations = applyCovid.TotalVaccinations + tvInt

		//Convert data type to int64.
		pvInt, _ := strconv.ParseInt(splitVal[len(splitVal)-1], 10, 64)
		//update PeopleVaccinated
		applyCovid.PeopleVaccinated = applyCovid.PeopleVaccinated + pvInt
	}

	covidDataVal := fmt.Sprintf("%s %d %d", applyCovid.IsoCode, applyCovid.TotalVaccinations, applyCovid.PeopleVaccinated)

	//length of all values.
	covidDataLen := len(covidDataVal)

	log.Info("Current covideData values: ", covidDataVal)

	log.Info("Write the KeyValue by calling PmdbWriteKV")
	rc := cso.pso.WriteKV(appId, pmdbHandle, applyCovid.Location,
		int64(keyLength), covidDataVal,
		int64(covidDataLen), colmfamily)

	return rc
}

func (cso *CovidServer) Read(appId unsafe.Pointer, requestBuf unsafe.Pointer,
	requestBufSize int64, replyBuf unsafe.Pointer, replyBufSize int64) int64 {

	log.Info("Covid19_Data App: Read request received")

	//Decode the request structure sent by client.
	reqStruct := &CovidAppLib.CovidLocale{}
	decodeErr := cso.pso.Decode(requestBuf, reqStruct, requestBufSize)

	if decodeErr != nil {
		log.Error("Failed to decode the read request")
		return -1
	}

	log.Info("Key passed by client: ", reqStruct.Location)

	keyLen := len(reqStruct.Location)
	log.Info("Key length: ", keyLen)

	/* Pass the work as key to PmdbReadKV and get the value from pumicedb */
	readRsult, readErr := cso.pso.ReadKV(appId, reqStruct.Location,
		int64(keyLen), colmfamily)

	var splitValues []string

	if readErr == nil {
		//split space separated values.
		splitValues = strings.Split(string(readRsult), " ")
	}

	//Convert TotalVaccinations and PeopleVaccinated into int64 type
	tvInt, _ := strconv.ParseInt(splitValues[1], 10, 64)
	pvInt, _ := strconv.ParseInt(splitValues[2], 10, 64)

	resultCovid := CovidAppLib.CovidLocale{
		Location:          reqStruct.Location,
		IsoCode:           splitValues[0],
		TotalVaccinations: tvInt,
		PeopleVaccinated:  pvInt,
	}

	//Copy the encoded result in replyBuffer
	replySize, copyErr := cso.pso.CopyDataToBuffer(resultCovid, replyBuf)
	if copyErr != nil {
		log.Error("Failed to Copy result in the buffer: %s", copyErr)
		return -1
	}

	log.Info("Reply size: ", replySize)

	return replySize
}
