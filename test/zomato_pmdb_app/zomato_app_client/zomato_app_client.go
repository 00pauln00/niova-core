package main

import (
        "encoding/csv"
        "fmt"
        "io"
        "log"
        "os"
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

	fmt.Println("Starting client...")

	//Start the client.
        pmdb := PumiceDB.PmdbStartClient(raft_uuid_go, peer_uuid_go)

	//Open the file.
	csvfile, err := os.Open(filename)
	if err != nil {
	       log.Fatalln("Couldn't open the csv file", err)
	}

	//Parse the file.
	r := csv.NewReader(csvfile)

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
		Restaurant_id_struct, err := strconv.ParseInt(record[0], 10, 64)
		if err != nil {
			fmt.Println(Restaurant_id_struct)
		}

		//Typecast Votes to int64.
		Votes_struct, err := strconv.ParseInt(record[5], 10, 64)
		if err != nil {
			fmt.Println(Votes_struct)
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

		length_of_struct := PumiceDB.GetStructSize(struct_data)
                fmt.Println("Length of the structure: ", length_of_struct)

		//Generate app_uuid.
                app_uuid := uuid.NewV4().String()

                //Create rncui string.
		rncui := app_uuid+":0:0:0:0"

		//Check for the operation.
	        if ops == "write"{
			//Perform write operation.
			PumiceDB.PmdbClientWrite(struct_data, pmdb, rncui)

		} else{
			fmt.Println("In read")

			//Get size of the structure struct_data.
			struct_len := PumiceDB.GetStructSize(struct_data)
			fmt.Println("Length of the struct_data: ", struct_len)

			// Allocate C memory to store the value of the result.
			struct_buf := C.malloc(C.size_t(struct_len))

			//Perform read operation
			PumiceDB.PmdbClientRead(struct_data, pmdb, rncui, struct_buf, int64(struct_len))

			//Data received after read request
			read_output_data := (*zomatoapplib.Zomato_App)(struct_buf)

			//converting read_output_data.Votes form int64 to int and then to string
			int_votes := int(read_output_data.Votes)
			str_votes := strconv.Itoa(int_votes)

			out := map[string]string{ "Restaurant_name": read_output_data.Restaurant_name,
						  "City": read_output_data.City,
					          "Cuisines": read_output_data.Cuisines,
						  "Ratings_text": read_output_data.Ratings_text,
						  "Votes": str_votes,
						}

			fmt.Println("Data received after read request...",out)
			C.free(struct_buf)
		}
	}
}

func main(){
	//Function call to start client and perform read /write
	Zomato_app_client()
}
