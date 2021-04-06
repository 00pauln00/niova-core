package main

import (
        "encoding/csv"
        "fmt"
        "os"
        "io"
	"bufio"
        "log"
	"strconv"
        "flag"
	"github.com/satori/go.uuid"
        "pmdblib/goPmdb"
        "covidapp.com/covidapplib"
)

/*
#cgo pkg-config: niova --define-variable=prefix=/usr/local/niova
#include <stdlib.h>
*/
import "C"

var raft_uuid_go string
var peer_uuid_go string
var ops string
var filename string

/*
 Start the pmdb client.
*/
func covidDataClient() {

        //Start the client.
        pmdb := PumiceDB.PmdbStartClient(raft_uuid_go, peer_uuid_go)

        // Open the file
        csvfile, err := os.Open(filename)
        if err != nil {
                log.Fatalln("Error to open the csv file", err)
        }

	// Skip first row (line)
	row1, err := bufio.NewReader(csvfile).ReadSlice('\n')
	if err != nil {
	  log.Fatalln("error")
	}
	_, err = csvfile.Seek(int64(len(row1)), io.SeekStart)
	if err != nil {
	   log.Fatalln("error")
	}

        // Parse the file
        r := csv.NewReader(csvfile)

	//Generate app_uuid.
        app_uuid := uuid.NewV4().String()

        //Create rncui string.
        rncui := app_uuid + ":0:0:0:0"

        for {
                // Read each record from csv
                record, err := r.Read()
                if err == io.EOF {
                        break
                }
                if err != nil {
                        log.Fatal(err)
                }

		//typecast the data type to int
		Total_vaccinations_int, _ := strconv.ParseInt(record[2], 10, 64)
                People_vaccinated_int, _ := strconv.ParseInt(record[3], 10, 64)

                //fill the struture
                covid := CovidAppLib.Covid_app{
                      Location: record[0],
                      Iso_code: record[1],
		      Total_vaccinations: Total_vaccinations_int,
		      People_vaccinated: People_vaccinated_int,
            }
            fmt.Printf("%s %s %d %d\n", covid.Location, covid.Iso_code,
                covid.Total_vaccinations, covid.People_vaccinated)
	    fmt.Println("Structure Data:", covid)
	    /*
            * Get the actual size of the structure
            */
            length_of_struct := PumiceDB.GetStructSize(covid)
            fmt.Println("Length of the structure: ", length_of_struct)

            if ops == "write"{
                fmt.Println("Write opeartion-Structure Data:", covid)
                //Perform write operation.
		PumiceDB.PmdbClientWrite(covid, pmdb, rncui)
            } else {
                   fmt.Println("Do read operation")
		   /*
                   * Get the actual size of the structure
                   */
                   length_of_struct := PumiceDB.GetStructSize(covid)
                   fmt.Println("Length of the structure: ", length_of_struct)

                   // Allocate C memory to store the value of the result.
                   value_buf := C.malloc(C.size_t(length_of_struct))

		   //Print all values which passed to PmdbClientRead()
		   fmt.Println("Covid Structure:", covid)
		   fmt.Println("ClientStart object:", pmdb)
		   fmt.Println("RNCUI:", rncui)
                   fmt.Println("Value buffer size:", value_buf)
		   fmt.Println("Length of the structure: ", int64(length_of_struct))

		   //read operation
                   PumiceDB.PmdbClientRead(covid, pmdb, rncui, value_buf, int64(length_of_struct))

                   struct_result := (*CovidAppLib.Covid_app)(value_buf)
		   fmt.Println("Struct Result:", struct_result)

                   fmt.Println("Result of the read request is:")
                   fmt.Println("Location: ", struct_result.Location)
                   C.free(value_buf)
            }
        }
}

func pmdb_dict_app_getopts() {

	flag.StringVar(&ops, "ops", "NULL", "ops")
        flag.StringVar(&filename, "filename", "NULL", "filename")
        flag.StringVar(&raft_uuid_go, "raft", "NULL", "raft uuid")
        flag.StringVar(&peer_uuid_go, "peer", "NULL", "peer uuid")

        flag.Parse()
        fmt.Println("Operation: ", ops)
        fmt.Println("File Name: ", filename)
	fmt.Println("Raft UUID: ", raft_uuid_go)
	fmt.Println("Peer UUID: ", peer_uuid_go)
}

func main() {
        //Parse the cmdline parameter
        pmdb_dict_app_getopts()

        //Start pmdbDictionary client.
        covidDataClient()
}


