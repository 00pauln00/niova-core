package main

import (
        "encoding/csv"
        "fmt"
        "os"
        "io"
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
 Read the request from console and process it.
 User will pass the request in following format.
 app_uuid.Text.write => For write opertion.
 app_uuid.Word.write  => For read operation.
*/
func covidDataClient() {

        //Start the client.
        pmdb := PumiceDB.PmdbStartClient(raft_uuid_go, peer_uuid_go)

        // Open the file
        csvfile, err := os.Open(filename)
        if err != nil {
                log.Fatalln("Error to open the csv file", err)
        }

        // Parse the file
        r := csv.NewReader(csvfile)

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

	    //Generate app_uuid.
            app_uuid := uuid.NewV4().String()

            //Create rncui string.
            rncui := app_uuid + ":0:0:0:0"

            if ops == "write"{
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

                   //read operation
                   PumiceDB.PmdbClientRead(covid, pmdb, rncui, value_buf, int64(length_of_struct))

                   struct_result := (*CovidAppLib.Covid_app)(value_buf)

                   fmt.Println("Result of the read request is:")
                   fmt.Println("Location: ", struct_result.Location)
                   //fmt.Println("Frequecy of the word: ", result_dict.Dict_wcount)
                   //C.free(value_buf)
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


