package main

import (
        "encoding/csv"
        "fmt"
	"time"
	"unsafe"
        "io"
	"strings"
        "log"
        "os"
	"bufio"
	"strconv"
        "gopmdblib/goPmdbClient"
	"github.com/satori/go.uuid"
        "zomatoapp/zomatoapplib"
)

/*
#include <stdlib.h>
*/
import "C"

func Zomato_app_apply(struct_app *zomatoapplib.Zomato_App, pmdb unsafe.Pointer){

		//Generate app_uuid.
		app_uuid := uuid.NewV4().String()

		//Create rncui string.
		rncui := app_uuid+":0:0:0:0"

		//Open file for storing key, rncui in append mode.
                file, err := os.OpenFile("key_rncui_data.txt", os.O_APPEND|os.O_WRONLY, 0644)
                    if err != nil {
                        log.Println(err)
                }
                defer file.Close()

		//Append key_rncui in file.
		rest_id_string := PumiceDBClient.GoIntToString(int(struct_app.Restaurant_id))
		if _, err := file.WriteString("key, rncui = "+rest_id_string+"  "+rncui+"\n"); err != nil {
		log.Fatal(err)
		}

		//Perform write operation.
		PumiceDBClient.PmdbClientWrite(struct_app, pmdb, rncui)

}

func Parse_file_and_apply(pmdb unsafe.Pointer, filename string){

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
				fmt.Println("Error occured in typecasting Restaurant_id to int64")
			}

			//Typecast Votes to int64.
			Votes_struct, err := strconv.ParseInt(record[5], 10, 64)
			if err != nil {
				fmt.Println("Error occured in typecasting Votes to int64")
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
			Zomato_app_apply(&struct_data, pmdb)

		}
}


func Zomato_app_read(pmdb unsafe.Pointer, key string, read_rncui string){


			var reply_len int64

			//Typecast key into int64.
			key_int64,_ := strconv.ParseInt(key, 10, 64)

			//Fill the Zomato_App structure.
			struct_read := zomatoapplib.Zomato_App {
				    Restaurant_id: key_int64,
			}

			//Get size of the structure struct_data.
			struct_len := PumiceDBClient.GetStructSize(struct_read)

			fmt.Println("Length of the struct_read: ", struct_len)

			//Allocate C memory to store the value of the result.
			struct_buf := C.malloc(65536)

			//Perform read operation.
			rc := PumiceDBClient.PmdbClientRead(struct_read, pmdb, read_rncui, struct_buf, 65536, &reply_len)
			if rc < 0 {
				fmt.Println("Read request failed, error: ", rc)
				fmt.Println("Reply length returned is: ", reply_len)
			} else {
				fmt.Println("Read the return data now")
				read_data := &zomatoapplib.Zomato_App{}
				PumiceDBClient.Decode(struct_buf, read_data, reply_len)

				fmt.Println("\nData received after read request:")
				fmt.Println("Restaurant id = ",read_data.Restaurant_id)
				fmt.Println("Restaurant name = ",read_data.Restaurant_name)
				fmt.Println("City = ",read_data.City)
				fmt.Println("Cuisines = ",read_data.Cuisines)
				fmt.Println("Ratings_text = ",read_data.Ratings_text)
				fmt.Println("Votes = ",read_data.Votes)

			}
			C.free(struct_buf)


}


func main(){

	//Accept raft and client uuid from cmdline.
	raft_uuid_go := os.Args[1]
	peer_uuid_go := os.Args[2]

	fmt.Println("Raft uuid:", raft_uuid_go)
	fmt.Println("Client uuid:", peer_uuid_go)

        fmt.Println("Starting client...")
	//Start the client.
	pmdb := PumiceDBClient.PmdbStartClient(raft_uuid_go, peer_uuid_go)

	//Sleep for 1 minute to start client properly.
	fmt.Println("Wait for client to bootup properly... 1 minute")
	time.Sleep(1*time.Minute)

	//Create a file for storing keys and rncui.
	os.Create("key_rncui_data.txt")

	fmt.Print("\nEnter filename for write operation: ")
	input := bufio.NewReader(os.Stdin)
	filename,_ := input.ReadString('\n')
	filename = strings.Replace(filename, "\n", "", -1)
	fmt.Println("Filename:", filename)

        //Perform write operation.
	Parse_file_and_apply(pmdb, filename)

	for {

		fmt.Print("Enter operation (write/read): ")
		ops,_ := input.ReadString('\n')
		ops = strings.Replace(ops, "\n", "", -1)

		if ops == "write"{

			fmt.Print("\nEnter zomato_data in the format - resturantid_name_city_cuisines_ratings_votes : ")
			data,_ := input.ReadString('\n')
			data = strings.Replace(data, "\n", "", -1)
			cmdline_prms := strings.Split(data, "_")

			//Typecast Restaurant_id to int64.
			rest_id_string := cmdline_prms[0]
			Restaurant_id_struct, err := strconv.ParseInt(rest_id_string, 10, 64)
			if err != nil {
				fmt.Println("Error occured in typecasting Restaurant_id to int64")
			}

			//Typecast Votes to int64.
			Votes_struct, err := strconv.ParseInt(cmdline_prms[5], 10, 64)
			if err != nil {
				fmt.Println("Error occured in typecasting Votes to int64")
			}

			//Fill the Zomato_App structure.
			struct_data_cmdline := zomatoapplib.Zomato_App {
					Restaurant_id: Restaurant_id_struct,
					Restaurant_name: cmdline_prms[1],
					City: cmdline_prms[2],
					Cuisines: cmdline_prms[3],
					Ratings_text: cmdline_prms[4],
					Votes: Votes_struct,
					}

			Zomato_app_apply(&struct_data_cmdline, pmdb)

		} else {

			fmt.Println("\nEnter key(Restuarant_id), rncui in the format - key_rncui (underscore seperated):")
			read_prms,_ := input.ReadString('\n')
			read_prms = strings.Replace(read_prms, "\n", "", -1)

			rprms := strings.Split(read_prms,"_")
			key := rprms[0]
			rncui := rprms[1]

			fmt.Println("Key :", key)
			fmt.Println("rucui:", rncui)

			//Perform read operation.
			Zomato_app_read(pmdb, key, rncui)

		}
	}

}
