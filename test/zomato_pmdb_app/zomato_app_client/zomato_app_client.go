package main
import (
  "encoding/csv"
  "fmt"
  "io"
  "flag"
  "strings"
  "log"
  "os"
  "bufio"
  "strconv"
  "gopmdblib/goPmdbClient"
  "gopmdblib/goPmdbCommon"
  "github.com/satori/go.uuid"
  "zomatoapp.com/zomatolib"
)

/*
#include <stdlib.h>
*/
import "C"

var raft_uuid_go string
var client_uuid_go string

func update(struct_app *zomatoapplib.Zomato_Data, client_obj *PumiceDBClient.PmdbClientObj){

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
	rest_id_string := strconv.Itoa(int(struct_app.Restaurant_id))
	_, err_write := file.WriteString("key, rncui = "+rest_id_string+"  "+rncui+"\n")
	if err_write != nil {
	   log.Fatal(err)
	}

	//Perform write operation.
	client_obj.PmdbClientWrite(struct_app, rncui)

}

func parse_file_and_update(client_obj *PumiceDBClient.PmdbClientObj, filename string){

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
		struct_data := zomatoapplib.Zomato_Data {
				Restaurant_id: Restaurant_id_struct,
				Restaurant_name: record[1],
				City: record[2],
				Cuisines: record[3],
				Ratings_text: record[4],
				Votes: Votes_struct,
				}
		update(&struct_data, client_obj)

	}
}


func get(client_obj *PumiceDBClient.PmdbClientObj, key string, read_rncui string){


	//Typecast key into int64.
	key_int64,_ := strconv.ParseInt(key, 10, 64)

	//Fill the Zomato_App structure.
	struct_read := zomatoapplib.Zomato_Data {
		    Restaurant_id: key_int64,
	}

	//Allocating size as 1024.
	struct_len := int64(1024)

	rc := -1
	/* Retry the read on failure */
	for ok := true; ok; ok = (rc < 0){

		fmt.Println("Allocating buffer of size: ", struct_len)
		//Allocate C memory to store the value of the result.
		struct_buf := C.malloc(C.size_t(struct_len))

		var reply_len int64
		//Perform read operation.
		rc := client_obj.PmdbClientRead(struct_read, read_rncui, struct_buf, int64(struct_len), &reply_len)
		if rc < 0 {
			fmt.Println("Read request failed, error: ", rc)
			fmt.Println("Reply length returned is: ", reply_len)
			if reply_len > struct_len{
                             fmt.Println("Allocate bigger buffer and retry read operation: ", struct_len)
                             struct_len = reply_len
			}
		} else {
			fmt.Println("Read the return data now")
			fmt.Println("Length of reply buffer:",reply_len)
			read_data := &zomatoapplib.Zomato_Data{}
			PumiceDBCommon.Decode(struct_buf, read_data, reply_len)

			fmt.Println("\nData received after read request:")
			fmt.Println("Restaurant id = ",read_data.Restaurant_id)
			fmt.Println("Restaurant name = ",read_data.Restaurant_name)
			fmt.Println("City = ",read_data.City)
			fmt.Println("Cuisines = ",read_data.Cuisines)
			fmt.Println("Ratings_text = ",read_data.Ratings_text)
			fmt.Println("Votes = ",read_data.Votes)
			break
		}
		C.free(struct_buf)
	}
}

func get_commandline_parameters(){

	flag.StringVar(&raft_uuid_go, "raft", "NULL", "raft uuid")
	flag.StringVar(&client_uuid_go, "peer", "NULL", "client uuid")

	flag.Parse()
	fmt.Println("Raft UUID: ", raft_uuid_go)
	fmt.Println("Client UUID: ", client_uuid_go)

}

func main(){

	//Print help message.
	if len(os.Args)==1 || os.Args[1] == "-help" || os.Args[1] == "-h"{
		fmt.Println("\nUsage: \n   For help:             ./zomato_app_client [-h] \n   To start client:      ./zomato_app_client -raft [raft_uuid] -peer [client_uuid]")
		fmt.Println("Positional Arguments: \n   raft_uuid \n   client_uuid")
		fmt.Println("Optional Arguments: \n   -h, --help            show this help message and exit")
                os.Exit(0)
	}

	//Accept raft and client uuid from cmdline.
	get_commandline_parameters()

        fmt.Println("Starting client...")
	//Start the client.
	pmdb := PumiceDBClient.PmdbStartClient(raft_uuid_go, client_uuid_go)

	obj := PumiceDBClient.PmdbClientObj{
               Pmdb: pmdb,
        }

        client_obj := &obj

	//Create a file for storing keys and rncui.
	os.Create("key_rncui_data.txt")

	fmt.Print("\nEnter filename for write operation (.csv file): ")
	input := bufio.NewReader(os.Stdin)
	filename,_ := input.ReadString('\n')
	filename = strings.Replace(filename, "\n", "", -1)
	fmt.Println("Filename:", filename)

        //Perform write operation.
	parse_file_and_update(client_obj, filename)

	for{
		fmt.Print("Enter operation (write/read/readall/exit): ")
		ops,_ := input.ReadString('\n')
		ops = strings.Replace(ops, "\n", "", -1)

		if ops == "write"{

			fmt.Print("\nEnter zomato_data in the format - resturantid_name_city_cuisines_ratingstext_votes : ")
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
			struct_data_cmdline := zomatoapplib.Zomato_Data {
					Restaurant_id: Restaurant_id_struct,
					Restaurant_name: cmdline_prms[1],
					City: cmdline_prms[2],
					Cuisines: cmdline_prms[3],
					Ratings_text: cmdline_prms[4],
					Votes: Votes_struct,
					}

			//Perform write operation.
			update(&struct_data_cmdline, client_obj)

		} else if ops == "read"{

			fmt.Println("\nEnter key(Restuarant_id), rncui in the format - key_rncui (underscore seperated):")
			read_prms,_ := input.ReadString('\n')
			read_prms = strings.Replace(read_prms, "\n", "", -1)

			rprms := strings.Split(read_prms,"_")
			key := rprms[0]
			rncui := rprms[1]

			fmt.Println("Key :", key)
			fmt.Println("rucui:", rncui)

			//Perform read operation.
			get(client_obj, key, rncui)

		} else if ops == "readall"{

			f, err := os.Open("key_rncui_data.txt")

			if err != nil {
			   log.Fatal(err)
			}

			defer f.Close()

			scanner := bufio.NewScanner(f)

			for scanner.Scan() {

				    rall_data := strings.Split(scanner.Text()," ")

				    rall_key := rall_data[3]
				    rall_rncui := rall_data[5]

				    fmt.Println("\nPerforming read for all key,values...key, rncui = ", rall_key,rall_rncui)

				    //Perform read operation for every key and rncui associated with it.
				    get(client_obj, rall_key, rall_rncui)
			}

			if err := scanner.Err(); err != nil {
				    log.Fatal(err)
			}
		} else if ops == "exit"{
			os.Exit(0)
		} else{
			fmt.Println("\nEnter valid operation: write/read/readall/exit")
		}
	}

}
