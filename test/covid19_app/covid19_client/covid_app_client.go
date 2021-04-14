package main

import (
	"encoding/csv"
	"unsafe"
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
	"covidapp.com/covidapplib"
)

/*
#include <stdlib.h>
*/
import "C"

var raft_uuid_go string
var peer_uuid_go string

func covidData_write(cli_start unsafe.Pointer, filename string) {

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
			covid_wr_struct := CovidAppLib.Covid_app{
			      Location: record[0],
			      Iso_code: record[1],
			      Total_vaccinations: Total_vaccinations_int,
			      People_vaccinated: People_vaccinated_int,
			}
			fmt.Printf("%s %s %d %d\n", covid_wr_struct.Location, covid_wr_struct.Iso_code,
					  covid_wr_struct.Total_vaccinations, covid_wr_struct.People_vaccinated)
			fmt.Println("Structure Data:", covid_wr_struct)

			//Get the actual size of the structure

			length_wr_struct := PumiceDBClient.GetStructSize(covid_wr_struct)
			fmt.Println("Length of the structure: ", length_wr_struct)

			fmt.Println("Write opeartion-Structure Data:", covid_wr_struct)
			//Perform write operation.
			PumiceDBClient.PmdbClientWrite(covid_wr_struct, cli_start, rncui)
		}
}

func covidData_write_struct(cli_start unsafe.Pointer, pass_input_struct interface{}, get_rncui string) {

		/*
		* Get the actual size of the structure
		*/
		length_wr_struct := PumiceDBClient.GetStructSize(pass_input_struct)
		fmt.Println("Length of the structure: ", length_wr_struct)

		fmt.Println("Write opeartion-Structure Data:", pass_input_struct)
		//Perform write operation.
		PumiceDBClient.PmdbClientWrite(pass_input_struct, cli_start, get_rncui)
}


func covidData_read(cli_start unsafe.Pointer, rd_input_key string, rd_rncui string) {

		covid_rd_struct := CovidAppLib.Covid_app {
			 Location : rd_input_key,
		}

		var reply_len int64
		length_rd_struct := PumiceDBClient.GetStructSize(covid_rd_struct) + 65536
		fmt.Println("Length of the structure: ", length_rd_struct)

		// Allocate C memory to store the value of the result.
		rd_value_buf := C.malloc(C.size_t(length_rd_struct))

		//Print all values which passed to PmdbClientRead()
		fmt.Println("Covid Structure:", covid_rd_struct)

		//read operation
		rc := PumiceDBClient.PmdbClientRead(covid_rd_struct, cli_start, rd_rncui, rd_value_buf,
							 int64(length_rd_struct), &reply_len)

		if rc < 0 {
			fmt.Println("Read request failed, error: ", rc)
			fmt.Println("Reply length returned is: ", reply_len)
		} else {
			fmt.Println("Read the return data now")
			struct_result := &CovidAppLib.Covid_app{}
			PumiceDBClient.Decode(rd_value_buf, struct_result, reply_len)

			fmt.Println("Result of the read request is:", struct_result)
			fmt.Println("Key is:", rd_input_key)

		}
		C.free(rd_value_buf)
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

		fmt.Println("Wait for 3 min to start client")
		cli_start := PumiceDBClient.PmdbStartClient(raft_uuid_go, peer_uuid_go)
		time.Sleep(3 * time.Minute)

		input_file := bufio.NewReader(os.Stdin)
		fmt.Print("Enter Filename: ")
		filename, _ := input_file.ReadString('\n')
		filename = strings.Replace(filename, "\n", "", -1)
		fmt.Println("Filename:", filename)

		//Write operation using file parsing
		covidData_write(cli_start, filename)


		fmt.Println("write rqst format ==> rncui#key#val0#val1#val2")
		fmt.Println("read rqst format ==> rncui#key")

		for {

			ops_key := bufio.NewReader(os.Stdin)
			fmt.Print("Enter operation: ")
			operation, _ := ops_key.ReadString('\n')
			operation = strings.Replace(operation, "\n", "", -1)

			if operation == "write" {
				//Read the key from console
				key := bufio.NewReader(os.Stdin)

				fmt.Print("Enter write rqst format: ")

				key_text, _ := key.ReadString('\n')

				// convert CRLF to LF
				key_text = strings.Replace(key_text, "\n", "", -1)
				input := strings.Split(key_text, "#")

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

				fmt.Println("write rncui: ", get_rncui)
				fmt.Println("Operation: ", operation)
				fmt.Println("Input Key: ", pass_input_struct.Location)
				fmt.Println("Input Value0: ", pass_input_struct.Iso_code)
				fmt.Println("Input Value1: ", pass_input_struct.Total_vaccinations)
				fmt.Println("Input Value2: ", pass_input_struct.People_vaccinated)

				//write operation
				covidData_write_struct(cli_start, pass_input_struct, get_rncui)
			} else {
				//Read the key from console
				rd_key := bufio.NewReader(os.Stdin)

				fmt.Print("Enter read rqst format: ")

				rd_key_text, _ := rd_key.ReadString('\n')

				// convert CRLF to LF
				rd_key_text = strings.Replace(rd_key_text, "\n", "", -1)
				rd_input := strings.Split(rd_key_text, "#")

				rd_rncui := rd_input[0]
				rd_input_key := rd_input[1]

				fmt.Println("read rncui: ", rd_rncui)
				fmt.Println("Operation: ", operation)
				fmt.Println("Read input Key: ", rd_input_key)

				//read operation
				covidData_read(cli_start, rd_input_key, rd_rncui)
			}
		}
}


