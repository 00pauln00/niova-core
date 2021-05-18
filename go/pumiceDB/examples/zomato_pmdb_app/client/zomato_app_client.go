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
	json_outfilepath string
	data             map[string]map[string]string
	zci              zomato_cli
)

// Creating an interface for client.
type zomato_cli interface {

	// Methods
	writeone([]string)
	write_multi(string) string
	get()
	copy_to_outfile(string)
}

//Structure declaration for a client request.
type go_pmdb_app_request struct {
	Operation     string
	Leader_uuid   string
	Outfile_uuid  string
	Key           string
	Rncui         string
	Json_filename string
	Cli_obj       *PumiceDBClient.PmdbClientObj
}

//Structure declaration to dump into json.
type zomato_app_info struct {
	Raft_uuid   string
	Client_uuid string
	Leader_uuid string
	Operation   string
	Status      int
	Timestamp   string
	Data        map[string]map[string]string
}

//Function to write write_data into map.
func fill_data_into_map(mp map[string]string, rncui string) {

	//Fill data into outer map.
	data[rncui] = mp

}

//Method to dump zomato_app_output structure into json file.
func (struct_out *zomato_app_info) dump_into_json(outf_uuid string) string {

	//Prepare path for temporary json file.
	temp_outfile_name := json_outfilepath + "/" + outf_uuid + ".json"
	file, _ := json.MarshalIndent(struct_out, "", "\t")
	_ = ioutil.WriteFile(temp_outfile_name, file, 0644)

	return temp_outfile_name

}

//Method to perform single write operation.
func (gopmdb_appstruct go_pmdb_app_request) writeone(args []string) {

	rncui := args[1]
	//Typecast Restaurant_id to int64.
	rest_id_string := args[2]
	restaurant_id_str, err := strconv.ParseInt(rest_id_string, 10, 64)
	if err != nil {
		fmt.Println("Error occured in typecasting Restaurant_id to int64")
	}

	//Typecast Votes to int64.
	votes_str, err := strconv.ParseInt(args[7], 10, 64)
	if err != nil {
		fmt.Println("Error occured in typecasting Votes to int64")
	}

	//Create a file for storing keys and rncui.
	os.Create("key_rncui_data.txt")

	//Get timestamp.
	timestamp := time.Now().Format("2006-01-02 15:04:05")

	//Fill the Zomato_App structure.
	struct_data_cmdline := zomatoapplib.Zomato_Data{
		Restaurant_id:   restaurant_id_str,
		Restaurant_name: args[3],
		City:            args[4],
		Cuisines:        args[5],
		Ratings_text:    args[6],
		Votes:           votes_str,
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

	//Perform write operation.
	rc := gopmdb_appstruct.Cli_obj.Write(struct_data_cmdline, rncui)
	if rc != 0 {
		fmt.Println("Pmdb Write failed.")
		write_strdata_cmd := zomato_app_info{
			Raft_uuid:   raft_uuid_go,
			Client_uuid: client_uuid_go,
			Leader_uuid: gopmdb_appstruct.Leader_uuid,
			Operation:   gopmdb_appstruct.Operation,
			Timestamp:   timestamp,
			Data:        data,
		}

		status := strconv.Itoa(int(rc))
		write_mp := map[string]string{
			"Key":    rest_id_string,
			"Status": status,
		}

		//Fill write request data into a map.
		fill_data_into_map(write_mp, rncui)

		//Dump structure into json.
		temp_outfname := write_strdata_cmd.dump_into_json(gopmdb_appstruct.Outfile_uuid)

		//Copy temporary json file into json outfile.
		gopmdb_appstruct.copy_to_outfile(temp_outfname)
	} else {
		fmt.Println("Pmdb Write successful!")
		write_strdata_cmd := zomato_app_info{
			Raft_uuid:   raft_uuid_go,
			Client_uuid: client_uuid_go,
			Leader_uuid: gopmdb_appstruct.Leader_uuid,
			Operation:   gopmdb_appstruct.Operation,
			Timestamp:   timestamp,
			Data:        data,
		}

		status := strconv.Itoa(int(rc))
		write_mp := map[string]string{
			"Key":    rest_id_string,
			"Status": status,
		}

		//Fill write request data into a map.
		fill_data_into_map(write_mp, rncui)

		//Dump structure into json.
		temp_outfname := write_strdata_cmd.dump_into_json(gopmdb_appstruct.Outfile_uuid)

		//Copy temporary json file into json outfile.
		gopmdb_appstruct.copy_to_outfile(temp_outfname)
	}

}

//Method for applying data.
func (gopmdb_appstruct go_pmdb_app_request) update(struct_app *zomatoapplib.Zomato_Data) string {

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

	var tmpoutfname string
	//Perform write operation.
	rc := gopmdb_appstruct.Cli_obj.Write(struct_app, rncui)

	if rc != 0 {
		fmt.Println("Pmdb Write failed.")
		write_strdata := zomato_app_info{
			Raft_uuid:   raft_uuid_go,
			Client_uuid: client_uuid_go,
			Leader_uuid: gopmdb_appstruct.Leader_uuid,
			Operation:   gopmdb_appstruct.Operation,
			Timestamp:   timestamp,
			Data:        data,
		}

		status := strconv.Itoa(int(rc))
		write_mp := map[string]string{
			"Key":    rest_id_string,
			"Status": status,
		}

		//Fill write request data into a map.
		fill_data_into_map(write_mp, rncui)
		//Dump structure into json.
		tmpoutfname = write_strdata.dump_into_json(gopmdb_appstruct.Outfile_uuid)
	} else {
		fmt.Println("Pmdb Write successful!")
		write_strdata := zomato_app_info{
			Raft_uuid:   raft_uuid_go,
			Client_uuid: client_uuid_go,
			Leader_uuid: gopmdb_appstruct.Leader_uuid,
			Operation:   gopmdb_appstruct.Operation,
			Timestamp:   timestamp,
			Data:        data,
		}
		status := strconv.Itoa(int(rc))
		write_mp := map[string]string{
			"Key":    rest_id_string,
			"Status": status,
		}

		//Fill write request data into a map.
		fill_data_into_map(write_mp, rncui)
		//Dump structure into json.
		tmpoutfname = write_strdata.dump_into_json(gopmdb_appstruct.Outfile_uuid)
	}
	return tmpoutfname
}

//Method for parsing .csv file and perform write multi operation.
func (gopmdb_appstruct go_pmdb_app_request) write_multi(filename string) string {

	var tmpout_fname string
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
		tmpout_fname = gopmdb_appstruct.update(&struct_data)
	}
	return tmpout_fname
}

//Method for read operation.
func (gopmdb_appstruct go_pmdb_app_request) get() {

	//Typecast key into int64.
	key_int64, _ := strconv.ParseInt(gopmdb_appstruct.Key, 10, 64)

	//Fill the Zomato_App structure.
	struct_read := zomatoapplib.Zomato_Data{
		Restaurant_id: key_int64,
	}

	var reply_len int64
	var tmpout_filename string
	timestamp := time.Now().Format("2006-01-02 15:04:05")

	//Perform read operation.
	reply_buff := gopmdb_appstruct.Cli_obj.Read(struct_read, gopmdb_appstruct.Rncui, &reply_len)

	if reply_buff == nil {
		fmt.Println("Read request failed !!")
		strdata := zomato_app_info{
			Raft_uuid:   raft_uuid_go,
			Client_uuid: client_uuid_go,
			Leader_uuid: gopmdb_appstruct.Leader_uuid,
			Operation:   gopmdb_appstruct.Operation,
			Status:      -1,
			Timestamp:   timestamp,
			Data:        nil,
		}
		//Dump structure into json.
		tmpout_filename = strdata.dump_into_json(gopmdb_appstruct.Outfile_uuid)

		//Copy temporary json file into json outfile.
		gopmdb_appstruct.copy_to_outfile(tmpout_filename)

	} else {
		read_data := &zomatoapplib.Zomato_Data{}
		gopmdb_appstruct.Cli_obj.Decode(reply_buff, read_data, reply_len)

		fmt.Println("\nResult of the read request is:")
		fmt.Println("Restaurant id (key) = ", read_data.Restaurant_id)
		fmt.Println("Restaurant name = ", read_data.Restaurant_name)
		fmt.Println("City = ", read_data.City)
		fmt.Println("Cuisines = ", read_data.Cuisines)
		fmt.Println("Ratings_text = ", read_data.Ratings_text)
		fmt.Println("Votes = ", read_data.Votes)

		rest_id := strconv.Itoa(int(read_data.Restaurant_id))
		rest_votes := strconv.Itoa(int(read_data.Votes))
		read_req_mp := map[string]string{
			"Restaurant_id":   rest_id,
			"Restaurant_name": read_data.Restaurant_name,
			"city":            read_data.City,
			"cuisines":        read_data.Cuisines,
			"ratings_text":    read_data.Ratings_text,
			"votes":           rest_votes,
		}

		//Fill write request data into a map.
		fill_data_into_map(read_req_mp, gopmdb_appstruct.Rncui)

		strdata := zomato_app_info{
			Raft_uuid:   raft_uuid_go,
			Client_uuid: client_uuid_go,
			Leader_uuid: gopmdb_appstruct.Leader_uuid,
			Operation:   gopmdb_appstruct.Operation,
			Status:      0,
			Timestamp:   timestamp,
			Data:        data,
		}

		//Dump structure into json.
		tmpout_filename = strdata.dump_into_json(gopmdb_appstruct.Outfile_uuid)
		//Copy temporary json file into json outfile.
		gopmdb_appstruct.copy_to_outfile(tmpout_filename)
	}
	C.free(reply_buff)
}

//Method to copy temporary json file into output json file.
func (gopmdb_appstruct go_pmdb_app_request) copy_to_outfile(tmp_outfile_name string) {

	//Prepare json output filepath.
	json_outf := json_outfilepath + "/" + gopmdb_appstruct.Json_filename + ".json"

	//Create output json file.
	os.Create(json_outf)

	//Copy temporary json file into output json file.
	_, err := exec.Command("cp", tmp_outfile_name, json_outf).Output()
	if err != nil {
		fmt.Print("%s", err)
	}

	//Remove temporary outfile after copying into json outfile.
	os.Remove(tmp_outfile_name)

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
		ops := ops_split[0]

		//Make the required maps.
		data = make(map[string]map[string]string)

		//Generate uuid for temporary json file.
		outf_uuid := uuid.NewV4().String()

		//Get leader uuid.
		leader_uuid := cli_obj.GetLeader()

		switch ops {

		case "WriteOne":
			jsonfilename := ops_split[8]
			zci = go_pmdb_app_request{
				Operation:     ops,
				Leader_uuid:   leader_uuid,
				Outfile_uuid:  outf_uuid,
				Json_filename: jsonfilename,
				Cli_obj:       cli_obj,
			}
			//Perform WriteOne operation.
			zci.writeone(ops_split)

		case "WriteMulti":
			csv_filepath := ops_split[1]
			jsonfilename := ops_split[2]

			//Create a file for storing keys and rncui.
			os.Create("key_rncui_data.txt")

			zci = go_pmdb_app_request{
				Operation:     ops,
				Leader_uuid:   leader_uuid,
				Outfile_uuid:  outf_uuid,
				Json_filename: jsonfilename,
				Cli_obj:       cli_obj,
			}

			//Perform write operation.
			tmoutfname := zci.write_multi(csv_filepath)

			//Copy temporary json file into json outfile.
			zci.copy_to_outfile(tmoutfname)

		case "ReadOne":
			key := ops_split[1]
			rncui := ops_split[2]
			jfilename := ops_split[3]

			fmt.Println("key :", key)
			fmt.Println("rucui:", rncui)

			zci = go_pmdb_app_request{
				Operation:     ops,
				Leader_uuid:   leader_uuid,
				Outfile_uuid:  outf_uuid,
				Key:           key,
				Rncui:         rncui,
				Json_filename: jfilename,
				Cli_obj:       cli_obj,
			}
			//Perform read operation.
			zci.get()

		case "ReadMulti":
			f, err := os.Open("key_rncui_data.txt")
			if err != nil {
				log.Fatal(err)
			}

			defer f.Close()

			jfilename := ops_split[1]
			scanner := bufio.NewScanner(f)

			for scanner.Scan() {

				rall_data := strings.Split(scanner.Text(), " ")

				rall_key := rall_data[3]
				rall_rncui := rall_data[5]

				fmt.Println("\nPerforming read for all key,values...key, rncui = ", rall_key, rall_rncui)

				//Perform read operation for every key and rncui associated with it.
				zci = go_pmdb_app_request{
					Operation:     ops,
					Leader_uuid:   leader_uuid,
					Outfile_uuid:  outf_uuid,
					Key:           rall_key,
					Rncui:         rall_rncui,
					Json_filename: jfilename,
					Cli_obj:       cli_obj,
				}
				//Perform read operation.
				zci.get()
			}

			if err := scanner.Err(); err != nil {
				log.Fatal(err)
			}
		case "get_leader":
			fmt.Println("Leader uuid is: ", leader_uuid)

			jfilename := ops_split[1]
			//Get timestamp.
			timestamp := time.Now().Format("2006-01-02 15:04:05")

			get_leader_uuid_struct := zomato_app_info{
				Raft_uuid:   raft_uuid_go,
				Client_uuid: client_uuid_go,
				Leader_uuid: leader_uuid,
				Operation:   ops,
				Timestamp:   timestamp,
			}

			//Dump structure into json.
			tempfname := get_leader_uuid_struct.dump_into_json(outf_uuid)

			zci = go_pmdb_app_request{
				Operation:     ops,
				Leader_uuid:   leader_uuid,
				Outfile_uuid:  outf_uuid,
				Json_filename: jfilename,
				Cli_obj:       cli_obj,
			}
			//Copy temporary json file into json outfile.
			zci.copy_to_outfile(tempfname)

		case "exit":
			os.Exit(0)
		case "default":
			fmt.Println("Enter valid operation: (WriteOne/ WriteMulti/ ReadOne/ ReadMulti/ get_leader /exit)")
		}
	}

}
