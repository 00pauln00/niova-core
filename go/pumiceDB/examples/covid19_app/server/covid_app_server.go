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
		fmt.Println("You need to pass the following arguments:")
		fmt.Println("Positional Arguments: \n		'-r' - RAFT UUID \n		'-u' - PEER UUID")
		fmt.Println("Optional Arguments: \n		'-l' - Log Dir Path \n		-h, -help")
		fmt.Println("Pass arguments in this format: \n		./covid_app_server -r RAFT UUID -u PEER UUID")
		os.Exit(0)
	}

	cso := parseFlag()

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
		log.Fatal(err)
	}
}

func parseFlag() *CovidServer {
	cso := &CovidServer{}

	flag.StringVar(&cso.raftUuid, "r", "NULL", "raft uuid")
	flag.StringVar(&cso.peerUuid, "u", "NULL", "peer uuid")
	flag.StringVar(&logDir, "l", "/tmp/covidAppLog", "log dir")

	flag.Parse()

	return cso
}

//If log directory is not exist it creates directory.
//and if dir path is not passed then it will create log file
//in "/tmp/covidAppLog" path.
func makeDirectoryIfNotExists() error {
	if _, err := os.Stat(logDir); os.IsNotExist(err) {
		return os.Mkdir(logDir, os.ModeDir|0755)
	}
	return nil
}

func initLogger(cso *CovidServer) {

	var filename string = logDir + "/" + cso.peerUuid + ".log"
	fmt.Println("logfile:", filename)
	// Create the log file if doesn't exist. And append to it if it already exists.
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	Formatter := new(log.TextFormatter)
	// You can change the Timestamp format. But you have to use the same date and time.
	// "2006-02-02 15:04:06" Works. If you change any digit, it won't work
	// ie "Mon Jan 2 15:04:05 MST 2006" is the reference time. You can't change it
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

func (cso *CovidServer) Apply(app_id unsafe.Pointer, input_buf unsafe.Pointer,
	input_buf_sz int64, pmdb_handle unsafe.Pointer) {

	log.Info("Covid19_Data app server: Apply request received")

	/* Decode the input buffer into structure format */
	apply_covid := &CovidAppLib.Covid_locale{}

	decode_err := cso.pso.Decode(input_buf, apply_covid, input_buf_sz)
	if decode_err != nil {
		log.Fatal("Failed to decode the application data")
	}

	log.Info("Key passed by client: ", apply_covid.Location)

	//length of key.
	len_of_key := len(apply_covid.Location)

	var preValue string

	//Lookup the key first
	prevResult, err := cso.pso.LookupKey(apply_covid.Location,
		int64(len_of_key), preValue,
		colmfamily)

	log.Info("Previous value of the key: ", prevResult)

	if err == nil {

		//Get Total_vaccinations value and People_vaccinated value by splitting prevResult.
		split_val := strings.Split(prevResult, " ")

		//Convert data type to int64.
		TV_int, _ := strconv.ParseInt(split_val[len(split_val)-2], 10, 64)
		//update Total_vaccinations.
		apply_covid.Total_vaccinations = apply_covid.Total_vaccinations + TV_int

		//Convert data type to int64.
		PV_int, _ := strconv.ParseInt(split_val[len(split_val)-1], 10, 64)
		//update People_vaccinated
		apply_covid.People_vaccinated = apply_covid.People_vaccinated + PV_int
	}

	/*
		Total_vaccinations and People_vaccinated are the int type value so
		Convert value to string type.
	*/
	TotalVaccinations := strconv.Itoa(int(apply_covid.Total_vaccinations))
	PeopleVaccinated := strconv.Itoa(int(apply_covid.People_vaccinated))

	//Merge the all values.
	covideData_values := apply_covid.Iso_code + " " + TotalVaccinations + " " + PeopleVaccinated

	//length of all values.
	covideData_len := len(covideData_values)

	log.Info("covideData_values: ", covideData_values)

	log.Info("Write the KeyValue by calling PmdbWriteKV")
	cso.pso.WriteKV(app_id, pmdb_handle, apply_covid.Location,
		int64(len_of_key), covideData_values,
		int64(covideData_len), colmfamily)

}

func (cso *CovidServer) Read(app_id unsafe.Pointer, request_buf unsafe.Pointer,
	request_bufsz int64, reply_buf unsafe.Pointer, reply_bufsz int64) int64 {

	log.Info("Covid19_Data App: Read request received")

	//Decode the request structure sent by client.
	req_struct := &CovidAppLib.Covid_locale{}
	decode_err := cso.pso.Decode(request_buf, req_struct, request_bufsz)

	if decode_err != nil {
		log.Fatal("Failed to decode the read request")
	}

	log.Info("Key passed by client: ", req_struct.Location)

	key_len := len(req_struct.Location)
	log.Info("Key length: ", key_len)

	/* Pass the work as key to PmdbReadKV and get the value from pumicedb */
	read_kv_result, read_err := cso.pso.ReadKV(app_id, req_struct.Location,
		int64(key_len), colmfamily)

	var split_values []string

	if read_err == nil {
		//split space separated values.
		split_values = strings.Split(read_kv_result, " ")

	}

	//Convert Total_vaccinations and People_vaccinated into int64 type
	TV_int, _ := strconv.ParseInt(split_values[1], 10, 64)
	PV_int, _ := strconv.ParseInt(split_values[2], 10, 64)

	result_covid := CovidAppLib.Covid_locale{
		Location:           req_struct.Location,
		Iso_code:           split_values[0],
		Total_vaccinations: TV_int,
		People_vaccinated:  PV_int,
	}

	//Copy the encoded result in reply_buffer
	reply_size, copy_err := cso.pso.CopyDataToBuffer(result_covid, reply_buf)
	if copy_err != nil {
		log.Fatal("Failed to Copy result in the buffer: %s", copy_err)
	}

	log.Info("Reply size: ", reply_size)

	return reply_size
}
