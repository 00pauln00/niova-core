package main

import (
        "encoding/csv"
        "fmt"
        "io"
        "log"
        "os"
	"bufio"
	"strconv"
        "gopmdblib/goPmdb"
	"github.com/satori/go.uuid"
        "zomatoapp/zomatoapplib"
)

/*
#cgo pkg-config: niova --define-variable=prefix=/usr/local/niova
#include <stdlib.h>
*/
import "C"

func Zomato_app_client(){

	//Get operation to be performed, filename, raft_uuid and peer_uuid from cmdline.
        ops := os.Args[1]
        filename := os.Args[2]

        raft_uuid_go := os.Args[3]
        peer_uuid_go := os.Args[4]


        fmt.Println("Operation to be performed:", ops)
        fmt.Println("Filename:", filename)
        fmt.Println("Raft uuid:", raft_uuid_go)
        fmt.Println("Peer uuid:", peer_uuid_go)

	//Create a file for storing keys and rncui.
	f, err := os.Create("key_rncui_data.txt")

	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	fmt.Println("Starting client...")

	//Start the client.
        pmdb := PumiceDB.PmdbStartClient(raft_uuid_go, peer_uuid_go)

	//Open the file.
	csvfile, err := os.Open(filename)
	if err != nil {
	       log.Fatalln("Couldn't open the csv file", err)
	}

	//Parse the file.
	// Skip first row (line)
	row1, err := bufio.NewReader(csvfile).ReadSlice('\n')
	if err != nil {
	  log.Fatalln("error")
	}
	_, err = csvfile.Seek(int64(len(row1)), io.SeekStart)
	if err != nil {
	   log.Fatalln("error")
	}

	//Read remaining rows.
	r := csv.NewReader(csvfile)

	//Open file for storing key, rncui in append mode.
	file, err := os.OpenFile("key_rncui_data.txt", os.O_APPEND|os.O_WRONLY, 0644)
            if err != nil {
                log.Println(err)
        }
	defer file.Close()

	//Iterate through the records.
	for {
		//Read each record from csv.
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		//Typecast Restaurant_id to int64.
		rest_id_string := record[0]
		Restaurant_id_struct, err := strconv.ParseInt(rest_id_string, 10, 64)
		if err != nil {
			fmt.Println("Error occured")
		}

		//Typecast Votes to int64.
		Votes_struct, err := strconv.ParseInt(record[5], 10, 64)
		if err != nil {
			fmt.Println("Error occured")
		}

		//Fill the Zomato_App structure.
		struct_data := zomatoapplib.Zomato_App {
				Restaurant_id: Restaurant_id_struct,
				Restaurant_name: record[1],
				City: record[2],
				Cuisines: record[3],
				Ratings_text: record[4],
				Votes: Votes_struct,
				}


		//Generate app_uuid.
                app_uuid := uuid.NewV4().String()

                //Create rncui string.
		rncui := app_uuid+":0:0:0:0"

		//Append key_rncui in file.
		if _, err := file.WriteString(rest_id_string+" : "+rncui+"\n"); err != nil {
                log.Fatal(err)
                }

		//Check for the operation.
	        if ops == "apply"{

			fmt.Println("struct_data: ",struct_data)
			//Perform write operation.
			PumiceDB.PmdbClientWrite(struct_data, pmdb, rncui)

		} else{
			fmt.Println("In read")

			//Accept zomato_app_key and rncui for read operation from cmdline.
			key_to_search := os.Args[5]
			read_rncui := os.Args[6]

			fmt.Println("key_to_search:",key_to_search)
			fmt.Println("rncui for read operation: ", read_rncui)

			fmt.Println("struct_data: ",struct_data)
			//Get size of the structure struct_data.
                        struct_len := PumiceDB.GetStructSize(struct_data)
                        fmt.Println("Length of the struct_data: ", struct_len)

                        //Allocate C memory to store the value of the result.
                        struct_buf := C.malloc(C.size_t(struct_len))

			//Perform read operation
                        PumiceDB.PmdbClientRead(struct_data, pmdb, read_rncui, struct_buf, int64(struct_len))

			//Data received after read request
                        //read_output_data := (*zomatoapplib.Zomato_App)(struct_buf)
			//fmt.Println("read_output_data:", read_output_data)
		      }

        }
}

func main(){
	//Function call to start client and perform read /write
	Zomato_app_client()
}
