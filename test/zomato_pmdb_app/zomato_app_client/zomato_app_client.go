package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/satori/go.uuid"
	"gopmdblib/goPmdbClient"
	"gopmdblib/goPmdbCommon"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
	"zomatoapp.com/zomatolib"
)

/*
#include <stdlib.h>
*/
import "C"

var (
	raft_uuid_go     string
	client_uuid_go   string
	leader_uuid      string
	json_outfilepath string
	app_data_map     = make(map[string]map[string]string)
	csv_filepath     string
	ops              string
)

//Output structure declaration to dump into json.
type zomato_app_output struct {
	Raft_uuid   string
	Client_uuid string
	Leader_uuid string
	Operation   string
	Status      int
	Timestamp   string
	App_data    map[string]map[string]string
}

//Function to write read request output into map.
func write_output_into_map(read_data *zomatoapplib.Zomato_Data, read_rncui string) {

	rest_id := strconv.Itoa(int(read_data.Restaurant_id))
	rest_votes := strconv.Itoa(int(read_data.Votes))
	mp := map[string]string{
		"Restaurant_id":   rest_id,
		"Restaurant_name": read_data.Restaurant_name,
		"city":            read_data.City,
		"cuisines":        read_data.Cuisines,
		"ratings_text":    read_data.Ratings_text,
		"votes":           rest_votes,
	}

	app_data_map[read_rncui] = mp
}

//Method to dump zomato_app_output structure into json file.
func (struct_out *zomato_app_output) dump_into_json() {

	file, _ := json.MarshalIndent(struct_out, "", "\t")
	outfile_name := json_outfilepath + "/" + "client_" + ops + "_" + client_uuid_go + ".json"
	_ = ioutil.WriteFile(outfile_name, file, 0644)

}

func update(struct_app *zomatoapplib.Zomato_Data, client_obj *PumiceDBClient.PmdbClientObj) {

	//Generate app_uuid.
	app_uuid := uuid.NewV4().String()

	//Create rncui string.
	rncui := app_uuid + ":0:0:0:0"

	//Get timestamp.
	timestamp := time.Now().Format("2006-01-02 15:04:05")

	//Open file for storing key, rncui in append mode.
	file, err := os.OpenFile("key_rncui_data.txt", os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}
	defer file.Close()

	//Append key_rncui in file.
	rest_id_string := strconv.Itoa(int(struct_app.Restaurant_id))
	_, err_write := file.WriteString("key, rncui = " + rest_id_string + "  " + rncui + "\n")
	if err_write != nil {
		log.Fatal(err)
	}

	//Get leader uuid.
	leader_uuid = client_obj.PmdbGetLeader()

	//Perform write operation.
	rc := client_obj.PmdbClientWrite(struct_app, rncui)

	if rc != 0 {
		fmt.Println("Pmdb Write failed.")
		write_strdata := zomato_app_output{
			Raft_uuid:   raft_uuid_go,
			Client_uuid: client_uuid_go,
			Leader_uuid: leader_uuid,
			Operation:   ops,
			Status:      rc,
			Timestamp:   timestamp,
			App_data:    nil,
		}

		//Dump structure into json.
		write_strdata.dump_into_json()
	} else {
		fmt.Println("Pmdb Write successful!")
		//Fill read reuqest output into map.
		write_output_into_map(struct_app, rncui)

		write_strdata := zomato_app_output{
			Raft_uuid:   raft_uuid_go,
			Client_uuid: client_uuid_go,
			Leader_uuid: leader_uuid,
			Operation:   ops,
			Status:      rc,
			Timestamp:   timestamp,
			App_data:    app_data_map,
		}

		//Dump structure into json.
		write_strdata.dump_into_json()
	}
}

func parse_file_and_update(client_obj *PumiceDBClient.PmdbClientObj, filename string) {

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
		restaurant_id_struct, err := strconv.ParseInt(record[0], 10, 64)
		if err != nil {
			fmt.Println("Error occured in typecasting Restaurant_id to int64")
		}

		//Typecast Votes to int64.
		votes_struct, err := strconv.ParseInt(record[5], 10, 64)
		if err != nil {
			fmt.Println("Error occured in typecasting Votes to int64")
		}

		//Fill the Zomato_App structure.
		struct_data := zomatoapplib.Zomato_Data{
			Restaurant_id:   restaurant_id_struct,
			Restaurant_name: record[1],
			City:            record[2],
			Cuisines:        record[3],
			Ratings_text:    record[4],
			Votes:           votes_struct,
		}
		update(&struct_data, client_obj)

	}
}

func get(client_obj *PumiceDBClient.PmdbClientObj, key string, read_rncui string) {

	//Typecast key into int64.
	key_int64, _ := strconv.ParseInt(key, 10, 64)

	//Fill the Zomato_App structure.
	struct_read := zomatoapplib.Zomato_Data{
		Restaurant_id: key_int64,
	}

	// Get the size of the structure
	struct_len := PumiceDBCommon.GetStructSize(struct_read)
	fmt.Println("Length of the structure: ", struct_read)

	//Get leader uuid.
	leader_uuid := client_obj.PmdbGetLeader()

	var reply_len int64
	timestamp := time.Now().Format("2006-01-02 15:04:05")

	//Perform read operation.
	reply_buff := client_obj.PmdbClientRead(struct_read, read_rncui, int64(struct_len), &reply_len)

	if reply_buff == nil {
		fmt.Println("Read request failed !!")
		strdata := zomato_app_output{
			Raft_uuid:   raft_uuid_go,
			Client_uuid: client_uuid_go,
			Leader_uuid: leader_uuid,
			Operation:   ops,
			Status:      -1,
			Timestamp:   timestamp,
			App_data:    nil,
		}

		//Dump structure into json.
		strdata.dump_into_json()
	} else {
		read_data := &zomatoapplib.Zomato_Data{}
		PumiceDBCommon.Decode(reply_buff, read_data, reply_len)

		fmt.Println("\nResult of the read request is:")
		fmt.Println("Restaurant id (key) = ", read_data.Restaurant_id)
		fmt.Println("Restaurant name = ", read_data.Restaurant_name)
		fmt.Println("City = ", read_data.City)
		fmt.Println("Cuisines = ", read_data.Cuisines)
		fmt.Println("Ratings_text = ", read_data.Ratings_text)
		fmt.Println("Votes = ", read_data.Votes)

		//Fill read reuqest output into map.
		write_output_into_map(read_data, read_rncui)

		strdata := zomato_app_output{
			Raft_uuid:   raft_uuid_go,
			Client_uuid: client_uuid_go,
			Leader_uuid: leader_uuid,
			Operation:   ops,
			Status:      0,
			Timestamp:   timestamp,
			App_data:    app_data_map,
		}

		//Dump structure into json.
		strdata.dump_into_json()
	}
	C.free(reply_buff)
}

func get_commandline_parameters() {

	flag.StringVar(&raft_uuid_go, "r", "NULL", "raft uuid")
	flag.StringVar(&client_uuid_go, "u", "NULL", "client uuid")
	flag.StringVar(&json_outfilepath, "l", "NULL", "json_outfilepath")

	flag.Parse()
	fmt.Println("Raft UUID: ", raft_uuid_go)
	fmt.Println("Client UUID: ", client_uuid_go)
	fmt.Println("Outfile path: ", json_outfilepath)

}

func main() {

	//Print help message.
	if len(os.Args) == 1 || os.Args[1] == "-help" || os.Args[1] == "--help" || os.Args[1] == "-h" {
		fmt.Println("\nUsage: \n   For help:             ./zomato_app_client [-h] \n   To start client:      ./zomato_app_client -r [raft_uuid] -u [client_uuid] -l [json_outfilepath]")
		fmt.Println("\nPositional Arguments: \n   -r    raft_uuid \n   -u    client_uuid \n   -l    json_outfilepath")
		fmt.Println("\nOptional Arguments: \n   -h, --help            show this help message and exit")
		os.Exit(0)
	}

	//Accept raft and client uuid from cmdline.
	get_commandline_parameters()

	fmt.Println("Starting client: ", client_uuid_go)
	//Start the client.
	pmdb := PumiceDBClient.PmdbStartClient(raft_uuid_go, client_uuid_go)

	obj := PumiceDBClient.PmdbClientObj{
		Pmdb: pmdb,
	}

	client_obj := &obj

	//Create a file for storing keys and rncui.
	os.Create("key_rncui_data.txt")

	for {
		fmt.Print("Enter operation (WriteOne/ WriteMulti/ ReadOne/ ReadMulti/ get_leader /exit): ")
		input := bufio.NewReader(os.Stdin)
		cmd, _ := input.ReadString('\n')
		cmd_s := strings.Replace(cmd, "\n", "", -1)
		ops_split := strings.Split(cmd_s, "#")

		ops = ops_split[0]
		if ops == "WriteOne" {

			fmt.Println("\nWriteOne request received:")
			fmt.Println("Parameters passed:", ops_split)
			rncui := ops_split[1]

			//Get timestamp.
			timestamp := time.Now().Format("2006-01-02 15:04:05")
			//Typecast Restaurant_id to int64.
			rest_id_string := ops_split[2]
			restaurant_id_struct, err := strconv.ParseInt(rest_id_string, 10, 64)
			if err != nil {
				fmt.Println("Error occured in typecasting Restaurant_id to int64")
			}

			//Typecast Votes to int64.
			votes_struct, err := strconv.ParseInt(ops_split[7], 10, 64)
			if err != nil {
				fmt.Println("Error occured in typecasting Votes to int64")
			}

			//Fill the Zomato_App structure.
			struct_data_cmdline := zomatoapplib.Zomato_Data{
				Restaurant_id:   restaurant_id_struct,
				Restaurant_name: ops_split[3],
				City:            ops_split[4],
				Cuisines:        ops_split[5],
				Ratings_text:    ops_split[6],
				Votes:           votes_struct,
			}

			//Open file for storing key, rncui in append mode.
			file, err := os.OpenFile("key_rncui_data.txt", os.O_APPEND|os.O_WRONLY, 0644)
			if err != nil {
				log.Println(err)
			}
			defer file.Close()

			//Append key_rncui in file.
			_, err_write := file.WriteString("key, rncui = " + rest_id_string + "  " + rncui + "\n")
			if err_write != nil {
				log.Fatal(err)
			}

			//Get leader uuid.
			leader_uuid := client_obj.PmdbGetLeader()

			//Perform write operation.
			rc := client_obj.PmdbClientWrite(struct_data_cmdline, rncui)
			if rc != 0 {
				fmt.Println("Pmdb Write failed.")
				write_strdata_cmd := zomato_app_output{
					Raft_uuid:   raft_uuid_go,
					Client_uuid: client_uuid_go,
					Leader_uuid: leader_uuid,
					Operation:   ops,
					Status:      rc,
					Timestamp:   timestamp,
					App_data:    nil,
				}
				//Dump structure into json.
				write_strdata_cmd.dump_into_json()
			} else {
				fmt.Println("Pmdb Write successful!")
				//Fill read reuqest output into map.
				write_output_into_map(&struct_data_cmdline, rncui)

				write_strdata_cmd := zomato_app_output{
					Raft_uuid:   raft_uuid_go,
					Client_uuid: client_uuid_go,
					Leader_uuid: leader_uuid,
					Operation:   ops,
					Status:      rc,
					Timestamp:   timestamp,
					App_data:    app_data_map,
				}
				//Dump structure into json.
				write_strdata_cmd.dump_into_json()
			}

		} else if ops == "WriteMulti" {
			fmt.Println("\nWriteMulti request received:")
			fmt.Println("Parameters passed:", ops_split)
			csv_filepath := ops_split[1]
			//Perform write operation.
			parse_file_and_update(client_obj, csv_filepath)

		} else if ops == "ReadOne" {

			fmt.Println("\nReadOne request received:")
			fmt.Println("Parameters passed:", ops_split)
			key := ops_split[1]
			rncui := ops_split[2]

			fmt.Println("key :", key)
			fmt.Println("rucui:", rncui)

			//Perform read operation.
			get(client_obj, key, rncui)

		} else if ops == "ReadMulti" {

			fmt.Println("\nReadMulti request received:")
			fmt.Println("Parameters passed:", ops_split)
			f, err := os.Open("key_rncui_data.txt")

			if err != nil {
				log.Fatal(err)
			}

			defer f.Close()

			scanner := bufio.NewScanner(f)

			for scanner.Scan() {

				rall_data := strings.Split(scanner.Text(), " ")

				rall_key := rall_data[3]
				rall_rncui := rall_data[5]

				fmt.Println("\nPerforming read for all key,values...key, rncui = ", rall_key, rall_rncui)

				//Perform read operation for every key and rncui associated with it.
				get(client_obj, rall_key, rall_rncui)
			}

			if err := scanner.Err(); err != nil {
				log.Fatal(err)
			}
		} else if ops == "get_leader" {
			leader_uuid = client_obj.PmdbGetLeader()
			fmt.Println("Leader uuid is: ", leader_uuid)

			//Get timestamp.
			timestamp := time.Now().Format("2006-01-02 15:04:05")

			get_leader_uuid_struct := zomato_app_output{
				Raft_uuid:   raft_uuid_go,
				Client_uuid: client_uuid_go,
				Leader_uuid: leader_uuid,
				Operation:   ops,
				Timestamp:   timestamp,
			}

			//Dump structure into json.
			get_leader_uuid_struct.dump_into_json()
		} else if ops == "exit" {
			os.Exit(0)
		} else {
			fmt.Println("Enter valid operation: (WriteOne/ WriteMulti/ ReadOne/ ReadMulti/ get_leader /exit)")
		}
	}

}
