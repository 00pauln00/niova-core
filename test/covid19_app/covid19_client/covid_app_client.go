package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"io"
	"bufio"
	"log"
	"strconv"
	"time"
	"flag"
	"strings"
	"github.com/satori/go.uuid"
	"gopmdblib/goPmdbClient"
	"gopmdblib/goPmdbCommon"
	"covidapp.com/covidapplib"
)

/*
#include <stdlib.h>
*/
import "C"

var raft_uuid_go string
var peer_uuid_go string

//Create and Initialize the map.
var key_rncui_map = make(map[string]string)

//This function returns rncui.
func get_rncui(covid_wr_struct *CovidAppLib.Covid_app) string {

	//Generate app_uuid.
	app_uuid := uuid.NewV4().String()

	//Create rncui string.
	rncui := app_uuid + ":0:0:0:0"

	key_rncui_map[covid_wr_struct.Location] = rncui

	return rncui
}

//read cmdline input.
func get_cmdline_input(input []string) []string {
	//Read the key from console
        key := bufio.NewReader(os.Stdin)

        key_text, _ := key.ReadString('\n')

        // convert CRLF to LF
        key_text = strings.Replace(key_text, "\n", "", -1)
        input = strings.Split(key_text, "#")

	return input
}

//parse csv file.
func parse_csv_file(filename string) (fp *csv.Reader) {

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
        fp = csv.NewReader(csvfile)

	return fp
}

//write operation for csv file parsing.
func covidData_write_by_csvfile(client_obj *PumiceDBClient.PmdbClientObj) {

	//Get filename from console.
	input_file := bufio.NewReader(os.Stdin)
        fmt.Print("Enter Filename(csv file): ")
        filename, _ := input_file.ReadString('\n')
        filename = strings.Replace(filename, "\n", "", -1)

	//call function to parse csv file.
	fp := parse_csv_file(filename)

	for {
		// Read each record from csv
		record, err := fp.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		//typecast the data type to int
		Total_vaccinations_int, _ := strconv.ParseInt(record[3], 10, 64)
		People_vaccinated_int, _ := strconv.ParseInt(record[4], 10, 64)

		//fill the struture
		covid_wr_struct := CovidAppLib.Covid_app{
		      Location: record[0],
		      Iso_code: record[1],
		      Total_vaccinations: Total_vaccinations_int,
		      People_vaccinated: People_vaccinated_int,
		}

		//Call the function to get rncui.
		rncui := get_rncui(&covid_wr_struct)

		//get length of struct size
		length_wr_struct := PumiceDBCommon.GetStructSize(covid_wr_struct)
		fmt.Println("Length of the structure: ", length_wr_struct)

		fmt.Println("Structure Data:", covid_wr_struct)

		//Write the key-value to pumicedb
		client_obj.PmdbClientWrite(covid_wr_struct, rncui)
	}
}

//function to write covidData.
func covidData_write(client_obj *PumiceDBClient.PmdbClientObj,
			pass_input_struct interface{}, get_rncui string) {

	//Get the actual size of the structure.
	len_wr_struct := PumiceDBCommon.GetStructSize(pass_input_struct)
	fmt.Println("Length of the structure: ", len_wr_struct)

	//Perform write operation.
	client_obj.PmdbClientWrite(pass_input_struct, get_rncui)
}

//write operation by passing cmdline data.
func write_covidData_key_value_from_cmdline(client_obj *PumiceDBClient.PmdbClientObj) {

	var str []string

	//Read the key from console.
	input := get_cmdline_input(str)

	get_rncui := input[0]
	input_key := input[1]
	field0_val := input[2]
	field1_val := input[3]
	field2_val := input[4]

	//typecast the data type to int
	field1_int, _ := strconv.ParseInt(field1_val, 10, 64)
	field2_int, _ := strconv.ParseInt(field2_val, 10, 64)

	/*
	Prepare the structure from values passed by user.
	fill the struture
	*/
	pass_input_struct := CovidAppLib.Covid_app{
		   Location: input_key,
		   Iso_code: field0_val,
		   Total_vaccinations: field1_int,
		   People_vaccinated: field2_int,
	}

	fmt.Println("rncui: ", get_rncui)
	fmt.Println("key: ", pass_input_struct.Location)
	fmt.Println("value0: ", pass_input_struct.Iso_code)
	fmt.Println("value1: ", pass_input_struct.Total_vaccinations)
	fmt.Println("value2: ", pass_input_struct.People_vaccinated)

	//call function to write from cmdline.
	covidData_write(client_obj, pass_input_struct, get_rncui)
}

//Read Data.
func read_covidData(client_obj *PumiceDBClient.PmdbClientObj,
			covid_rd_struct interface{},
			key string, rd_rncui string) {

	length_rd_struct := PumiceDBCommon.GetStructSize(covid_rd_struct)

	//Print all values which passed to PmdbClientRead()
	fmt.Println("Covid Structure:", covid_rd_struct)

	rc := -1
        /* Retry the read on failure */
        for ok := true; ok; ok = (rc < 0) {

                // Allocate C memory to store the value of the result.
                fmt.Println("Allocating buffer of size: ", length_rd_struct)
                val_buf := C.malloc(65536)

		var reply_size int64
                //read operation
                rc = client_obj.PmdbClientRead(covid_rd_struct, rd_rncui, val_buf, 65536, &reply_size)

                if rc < 0 {
                        fmt.Println("Read request failed, error: ", rc)
                        if reply_size > length_rd_struct {
                                fmt.Println("Allocate bigger buffer and retry read operation: ", length_rd_struct)
                                length_rd_struct = reply_size
                        }
                } else {
                        fmt.Println("Read the return data now")
                        struct_op := &CovidAppLib.Covid_app{}
                        PumiceDBCommon.Decode(val_buf, struct_op, reply_size)
                        fmt.Println("Result of the read request is:", struct_op)
                }
                C.free(val_buf)
        }

}

func read_covidData_key_value_from_cmdline(client_obj *PumiceDBClient.PmdbClientObj) {

	var str []string

        //Read the key from console.
        input := get_cmdline_input(str)

	key := input[0]
	rd_rncui := input[1]

	covid_rd_struct := CovidAppLib.Covid_app{
			Location: key,
	}

	fmt.Println("key: ", key)
	fmt.Println("rncui: ", rd_rncui)

	//call function to read cmdline data.
	read_covidData(client_obj, covid_rd_struct, key, rd_rncui)
}

func pmdb_dict_app_getopts() {

	flag.StringVar(&raft_uuid_go, "raft", "NULL", "raft uuid")
	flag.StringVar(&peer_uuid_go, "peer", "NULL", "peer uuid")

	flag.Parse()
	fmt.Println("Raft UUID: ", raft_uuid_go)
	fmt.Println("Peer UUID: ", peer_uuid_go)
}

func main() {

	//Parse the cmdline parameter
	pmdb_dict_app_getopts()

	// sleep for 2sec.
	fmt.Println("Wait for 2 sec to start client")
	cli_start := PumiceDBClient.PmdbStartClient(raft_uuid_go, peer_uuid_go)
	time.Sleep(2 * time.Second)

	obj := PumiceDBClient.PmdbClientObj{
		Pmdb: cli_start,
	}

	client_obj := &obj

	//Write operation using csv file parsing.
	covidData_write_by_csvfile(client_obj)

	for {

		ops_key := bufio.NewReader(os.Stdin)
		fmt.Print("Enter operation(write/read/read_file: ")
		operation, _ := ops_key.ReadString('\n')
		operation = strings.Replace(operation, "\n", "", -1)

		if operation == "write" {

			fmt.Println("write rqst format ==> rncui#key#val0#val1#val2")
			fmt.Print("Enter write rqst format: ")
			//call function to write covid19 data from cmdline.
			write_covidData_key_value_from_cmdline(client_obj)

		} else if operation == "read" {

			fmt.Println("read rqst format ==> key#rncui")
			fmt.Print("Enter read rqst format: ")
			//call function to read cmdline data.
			read_covidData_key_value_from_cmdline(client_obj)

		} else if operation == "read_file" {
			//Iterate over map to read all data from csv file.
			for key, rd_rncui := range key_rncui_map {
				fmt.Println(key, " ", rd_rncui)
				covid_rd_struct := CovidAppLib.Covid_app{
					Location: key,
				}
				//call function to read csv file.
				read_covidData(client_obj, covid_rd_struct,
						key, rd_rncui)
			}
		} else {
			os.Exit(0)
		}
	}
}


