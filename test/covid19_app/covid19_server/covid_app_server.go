package main

import (
	"covidapp.com/covidapplib"
	"flag"
	"fmt"
	"gopmdblib/goPmdbCommon"
	"gopmdblib/goPmdbServer"
	"os"
	"strconv"
	"strings"
	"unsafe"
)

/*
#include <stdlib.h>
#include <string.h>
*/
import "C"

var seqno = 0
var raft_uuid_go string
var peer_uuid_go string

// Use the default column family
var colmfamily = "PMDBTS_CF"

func covid19_apply(app_id unsafe.Pointer, input_buf unsafe.Pointer,
	input_buf_sz int64, pmdb_handle unsafe.Pointer) {

	fmt.Println("Covid19_Data app server: Apply request received")

	/* Decode the input buffer into structure format */
	apply_covid := &CovidAppLib.Covid_app{}

	PumiceDBCommon.Decode(input_buf, apply_covid, input_buf_sz)

	fmt.Println("Key passed by client:", apply_covid.Location)

	//length of key.
	len_of_key := len(apply_covid.Location)

	var preValue string
	//var preValuePV string

	//Lookup the key first
	prevResult := PumiceDBServer.PmdbLookupKey(apply_covid.Location,
		int64(len_of_key), preValue,
		colmfamily)

	if prevResult != "" {
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
	TotalVaccinations := PumiceDBServer.GoIntToString(int(apply_covid.Total_vaccinations))
	PeopleVaccinated := PumiceDBServer.GoIntToString(int(apply_covid.People_vaccinated))

	//Merge the all values.
	covideData_values := apply_covid.Iso_code + " " + TotalVaccinations + " " + PeopleVaccinated

	//length of all values.
	covideData_len := PumiceDBServer.GoStringLen(covideData_values)

	fmt.Println("covideData_values: ", covideData_values)

	fmt.Println("Write the KeyValue by calling PmdbWriteKV")
	PumiceDBServer.PmdbWriteKV(app_id, pmdb_handle, apply_covid.Location,
		int64(len_of_key), covideData_values,
		int64(covideData_len), colmfamily)

}

func covid19_read(app_id unsafe.Pointer, request_buf unsafe.Pointer,
	request_bufsz int64, reply_buf unsafe.Pointer, reply_bufsz int64) int64 {

	fmt.Println("Covid19_Data App: Read request received")

	//Decode the request structure sent by client.
	req_struct := &CovidAppLib.Covid_app{}
	PumiceDBCommon.Decode(request_buf, req_struct, request_bufsz)

	fmt.Println("Key passed by client: ", req_struct.Location)

	key_len := len(req_struct.Location)
	fmt.Println("Key length: ", key_len)

	/* Pass the work as key to PmdbReadKV and get the value from pumicedb */
	read_kv_result := PumiceDBServer.PmdbReadKV(app_id, req_struct.Location,
		int64(key_len), colmfamily)

	//split space separated values.
	split_values := strings.Split(read_kv_result, " ")

	//Convert Total_vaccinations and People_vaccinated into int64 type
	TV_int, _ := strconv.ParseInt(split_values[1], 10, 64)
	PV_int, _ := strconv.ParseInt(split_values[2], 10, 64)

	result_covid := CovidAppLib.Covid_app{
		Location:           req_struct.Location,
		Iso_code:           split_values[0],
		Total_vaccinations: TV_int,
		People_vaccinated:  PV_int,
	}

	//Copy the encoded result in reply_buffer
	reply_size := PumiceDBServer.PmdbCopyDataToBuffer(result_covid, reply_buf)

	fmt.Println("Reply buffer size:", reply_bufsz)

	return reply_size
}

func pmdb_dict_app_getopts() {

	flag.StringVar(&raft_uuid_go, "r", "NULL", "raft uuid")
	flag.StringVar(&peer_uuid_go, "u", "NULL", "peer uuid")

	flag.Parse()
	fmt.Println("Raft UUID: ", raft_uuid_go)
	fmt.Println("Peer UUID: ", peer_uuid_go)
}

func main() {

	//Print help message.
	if len(os.Args) == 1 || os.Args[1] == "-help" || os.Args[1] == "-h" {
		fmt.Println("You need to pass the following arguments:")
		fmt.Println("Positional Arguments: \n           '-r' - RAFT UUID \n           '-u' - PEER UUID")
		fmt.Println("Optional Arguments: \n             -h, -help")
		fmt.Println("Pass arguments in this format: \n          ./covid_app_server -r RAFT UUID -u PEER UUID")
		os.Exit(0)
	}

	//Parse the cmdline parameters
	pmdb_dict_app_getopts()

	//Initialize the covid19_data application callback functions
	cb := &PumiceDBServer.PmdbCallbacks{
		ApplyCb: covid19_apply,
		ReadCb:  covid19_read,
	}

	PumiceDBServer.PmdbStartServer(raft_uuid_go, peer_uuid_go, colmfamily, cb)
}
