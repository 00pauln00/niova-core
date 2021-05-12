package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/satori/go.uuid"
	"io"
	"io/ioutil"
	"log"
	"niova/go-pumicedb-lib/client"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
	"zomatoapplib/lib"
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
	app_data_map     map[string]map[string]string
	data             map[string]map[string]int
	csv_filepath     string
	ops              string
	filename         string
	temp_uuid        string
	outfile_name     string
)

//Output structure declaration to dump into json.
type zomato_app_output struct {
	Raft_uuid   string
	Client_uuid string
	Leader_uuid string
	Operation   string
	Status      int
	Timestamp   string
	Data        map[string]map[string]string
}

//Structure for dumping write request data into json.
type zomato_app_write struct {
	Raft_uuid   string
	Client_uuid string
	Leader_uuid string
	Operation   string
	Timestamp   string
	Data        map[string]map[string]int
}

//Function to write write_data into map.
func write_reqdata_into_map(key, status int, rncui string) {

	write_mp := map[string]int{
		"Key":    key,
		"Status": status,
	}
	data[rncui] = write_mp

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

//Method to dump zomato_app_write structure into json file.
func (struct_out *zomato_app_write) dump_writereq_into_json() {

	//Prepare path for temporary json file.
	outfile_name = json_outfilepath + "/" + temp_uuid + ".json"

	file, _ := json.MarshalIndent(struct_out, "", "\t")
	_ = ioutil.WriteFile(outfile_name, file, 0644)

}

//Method to dump zomato_app_output structure into json file.
func (struct_out *zomato_app_output) dump_into_json() {

	//Prepare json output filepath.
	read_json_outf := json_outfilepath + "/" + filename + ".json"

	file, _ := json.MarshalIndent(struct_out, "", "\t")
	_ = ioutil.WriteFile(read_json_outf, file, 0644)

}

//Function for applying data.
func update(struct_app *zomatoapplib.Zomato_Data, cli_obj *PumiceDBClient.PmdbClientObj) {

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
	leader_uuid = cli_obj.GetLeader()

	//Perform write operation.
	rc := cli_obj.Write(struct_app, rncui)

	if rc != 0 {
		fmt.Println("Pmdb Write failed.")
		write_strdata := zomato_app_write{
			Raft_uuid:   raft_uuid_go,
			Client_uuid: client_uuid_go,
			Leader_uuid: leader_uuid,
			Operation:   ops,
			Timestamp:   timestamp,
			Data:        data,
		}

		//Fill key, status in map.
		write_reqdata_into_map(int(struct_app.Restaurant_id), rc, rncui)
		//Dump structure into json.
		write_strdata.dump_writereq_into_json()

	} else {
		fmt.Println("Pmdb Write successful!")
		write_strdata := zomato_app_write{
			Raft_uuid:   raft_uuid_go,
			Client_uuid: client_uuid_go,
			Leader_uuid: leader_uuid,
			Operation:   ops,
			Timestamp:   timestamp,
			Data:        data,
		}

		//Fill key, status in map.
		write_reqdata_into_map(int(struct_app.Restaurant_id), rc, rncui)
		//Dump structure into json.
		write_strdata.dump_writereq_into_json()
	}
}

//Function for parsing .csv file.
func parse_file_and_update(cli_obj *PumiceDBClient.PmdbClientObj, filename string) {

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
		update(&struct_data, cli_obj)

	}
}

//Function for read operation.
func get(cli_obj *PumiceDBClient.PmdbClientObj, key string, read_rncui string) {

	//Typecast key into int64.
	key_int64, _ := strconv.ParseInt(key, 10, 64)

	//Fill the Zomato_App structure.
	struct_read := zomatoapplib.Zomato_Data{
		Restaurant_id: key_int64,
	}

	//Get leader uuid.
	leader_uuid := cli_obj.GetLeader()

	var reply_len int64
	timestamp := time.Now().Format("2006-01-02 15:04:05")

	//Perform read operation.
	reply_buff := cli_obj.Read(struct_read, read_rncui, &reply_len)

	if reply_buff == nil {
		fmt.Println("Read request failed !!")
		strdata := zomato_app_output{
			Raft_uuid:   raft_uuid_go,
			Client_uuid: client_uuid_go,
			Leader_uuid: leader_uuid,
			Operation:   ops,
			Status:      -1,
			Timestamp:   timestamp,
			Data:        nil,
		}

		//Dump structure into json.
		strdata.dump_into_json()

	} else {
		read_data := &zomatoapplib.Zomato_Data{}
		cli_obj.Decode(reply_buff, read_data, reply_len)

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
			Data:        app_data_map,
		}

		//Dump structure into json.
		strdata.dump_into_json()

	}
	C.free(reply_buff)
}

//Function to copy temporary json file into output json file.
func copy_to_outfile() {

	//Prepare json output filepath.
	json_outf := json_outfilepath + "/" + filename + ".json"

	//Create output json file.
	os.Create(json_outf)

	//Copy temporary json file into output json file.
	_, err := exec.Command("cp", outfile_name, json_outf).Output()
	if err != nil {
		fmt.Printf("%s", err)
	}

	//Remove temporary outfile after copying into json outfile.
	os.Remove(outfile_name)

}

//Function to get command line parameters while starting of the client.
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

	//Create new client object.
	cli_obj := PumiceDBClient.PmdbClientNew(raft_uuid_go, client_uuid_go)
	if cli_obj == nil {
		return
	}

	fmt.Println("Starting client: ", client_uuid_go)
	//Start the client.
	cli_obj.Start()
	defer cli_obj.Stop()

	fmt.Print("\n**********Format for performing operations**********")
	fmt.Print("\nFor WriteOne Operation   => WriteOne#rncui#restaurant id#restaurant name#city#cuisines#ratings text#votes#outfilename")
	fmt.Print("\nFor ReadOne Operation    => ReadOne#key#rncui#outfilename")
	fmt.Print("\nFor WriteMulti Operation => WriteMulti#filename(.csv)#outfilename")
	fmt.Print("\nFor ReadMulti Operation  => ReadMulti#outfilename")
	fmt.Print("\nFor Get Leader Operation => get_leader#outfilename")

	for {
		fmt.Print("\nEnter operation (WriteOne/ WriteMulti/ ReadOne/ ReadMulti/ get_leader /exit): ")
		input := bufio.NewReader(os.Stdin)
		cmd, _ := input.ReadString('\n')
		cmd_s := strings.Replace(cmd, "\n", "", -1)
		ops_split := strings.Split(cmd_s, "#")
		ops = ops_split[0]

		//Make the required maps.
		data = make(map[string]map[string]int)
		app_data_map = make(map[string]map[string]string)

		if ops == "WriteOne" {

			rncui := ops_split[1]
			filename = ops_split[8]

			//Create uuid for temporary json file.
			temp_uuid = uuid.NewV4().String()

			//Create a file for storing keys and rncui.
			os.Create("key_rncui_data.txt")

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
			leader_uuid := cli_obj.GetLeader()

			//Perform write operation.
			rc := cli_obj.Write(struct_data_cmdline, rncui)
			if rc != 0 {
				fmt.Println("Pmdb Write failed.")
				write_strdata_cmd := zomato_app_write{
					Raft_uuid:   raft_uuid_go,
					Client_uuid: client_uuid_go,
					Leader_uuid: leader_uuid,
					Operation:   ops,
					Timestamp:   timestamp,
					Data:        data,
				}

				//Fill write request data into a map.
				write_reqdata_into_map(int(struct_data_cmdline.Restaurant_id), rc, rncui)

				//Dump structure into json.
				write_strdata_cmd.dump_writereq_into_json()

				//Copy temporary json file into json outfile.
				copy_to_outfile()
			} else {
				fmt.Println("Pmdb Write successful!")
				write_strdata_cmd := zomato_app_write{
					Raft_uuid:   raft_uuid_go,
					Client_uuid: client_uuid_go,
					Leader_uuid: leader_uuid,
					Operation:   ops,
					Timestamp:   timestamp,
					Data:        data,
				}

				//Fill write request data into a map.
				write_reqdata_into_map(int(struct_data_cmdline.Restaurant_id), rc, rncui)

				//Dump structure into json.
				write_strdata_cmd.dump_writereq_into_json()

				//Copy temporary json file into json outfile.
				copy_to_outfile()
			}

		} else if ops == "WriteMulti" {

			csv_filepath := ops_split[1]
			filename = ops_split[2]

			//Create uuid for temporary json file.
			temp_uuid = uuid.NewV4().String()

			//Create a file for storing keys and rncui.
			os.Create("key_rncui_data.txt")

			//Perform write operation.
			parse_file_and_update(cli_obj, csv_filepath)

			//Copy temporary json file into json outfile.
			copy_to_outfile()

		} else if ops == "ReadOne" {

			key := ops_split[1]
			rncui := ops_split[2]
			filename = ops_split[3]

			fmt.Println("key :", key)
			fmt.Println("rucui:", rncui)

			//Perform read operation.
			get(cli_obj, key, rncui)

		} else if ops == "ReadMulti" {

			f, err := os.Open("key_rncui_data.txt")

			filename = ops_split[1]

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
				get(cli_obj, rall_key, rall_rncui)
			}

			if err := scanner.Err(); err != nil {
				log.Fatal(err)
			}
		} else if ops == "get_leader" {
			leader_uuid = cli_obj.GetLeader()
			fmt.Println("Leader uuid is: ", leader_uuid)

			filename = ops_split[1]
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
