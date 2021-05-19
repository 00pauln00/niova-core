package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/satori/go.uuid"
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
	Prepare()
	Exec()
	Complete()
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

type WriteOne struct {
	Args         []string
	Leader_uuid  string
	Outfile_uuid string
	Cli_obj      *PumiceDBClient.PmdbClientObj
	Zomato_data  *zomatoapplib.Zomato_Data
	Outfilename  string
}

type ReadOne struct {
	Key           string
	Rncui         string
	Json_filename string
	Operation     string
	Leader_uuid   string
	Outfile_uuid  string
	Outfilename   string
	Cli_obj       *PumiceDBClient.PmdbClientObj
	Zomato_data   *zomatoapplib.Zomato_Data
}

func (prep_strct *WriteOne) Prepare() {
	rncui := prep_strct.Args[1]
	//Typecast Restaurant_id to int64.
	rest_id_string := prep_strct.Args[2]
	restaurant_id_str, err := strconv.ParseInt(rest_id_string, 10, 64)
	if err != nil {
		fmt.Println("Error occured in typecasting Restaurant_id to int64")
	}

	//Typecast Votes to int64.
	votes_str, err := strconv.ParseInt(prep_strct.Args[7], 10, 64)
	if err != nil {
		fmt.Println("Error occured in typecasting Votes to int64")
	}

	//Create a file for storing keys and rncui.
	os.Create("key_rncui_data.txt")

	//Fill the Zomato_App structure.
	struct_data_cmdline := zomatoapplib.Zomato_Data{
		Restaurant_id:   restaurant_id_str,
		Restaurant_name: prep_strct.Args[3],
		City:            prep_strct.Args[4],
		Cuisines:        prep_strct.Args[5],
		Ratings_text:    prep_strct.Args[6],
		Votes:           votes_str,
	}

	prep_strct.Zomato_data = &struct_data_cmdline
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
}

func (exec_strct *WriteOne) Exec() {

	var temp_outfname string
	//Get timestamp.
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	//Perform write operation.
	rc := exec_strct.Cli_obj.Write(exec_strct.Zomato_data, exec_strct.Args[1])
	if rc != 0 {
		fmt.Println("Pmdb Write failed.")
		write_strdata_cmd := zomato_app_info{
			Raft_uuid:   raft_uuid_go,
			Client_uuid: client_uuid_go,
			Leader_uuid: exec_strct.Leader_uuid,
			Operation:   exec_strct.Args[0],
			Timestamp:   timestamp,
			Data:        data,
		}

		status := strconv.Itoa(int(rc))
		write_mp := map[string]string{
			"Key":    exec_strct.Args[2],
			"Status": status,
		}

		//Fill write request data into a map.
		fill_data_into_map(write_mp, exec_strct.Args[1])

		//Dump structure into json.
		temp_outfname = write_strdata_cmd.dump_into_json(exec_strct.Outfile_uuid)

	} else {
		fmt.Println("Pmdb Write successful!")
		write_strdata_cmd := zomato_app_info{
			Raft_uuid:   raft_uuid_go,
			Client_uuid: client_uuid_go,
			Leader_uuid: exec_strct.Leader_uuid,
			Operation:   exec_strct.Args[0],
			Timestamp:   timestamp,
			Data:        data,
		}

		status := strconv.Itoa(int(rc))
		write_mp := map[string]string{
			"Key":    exec_strct.Args[2],
			"Status": status,
		}

		//Fill write request data into a map.
		fill_data_into_map(write_mp, exec_strct.Args[1])

		//Dump structure into json.
		temp_outfname = write_strdata_cmd.dump_into_json(exec_strct.Outfile_uuid)
	}
	exec_strct.Outfilename = temp_outfname
}

func (com_strct *WriteOne) Complete() {
	//Copy contents in json outfile.
	copy_to_outfile(com_strct.Outfilename, com_strct.Args[8])
}

func (prep_rstrct *ReadOne) Prepare() {

	//Typecast key into int64.
	key_int64, _ := strconv.ParseInt(prep_rstrct.Key, 10, 64)
	//Fill the Zomato_App structure.
	struct_read := zomatoapplib.Zomato_Data{
		Restaurant_id: key_int64,
	}
	prep_rstrct.Zomato_data = &struct_read
}

func (prep_rstrct *ReadOne) Exec() {

	var reply_len int64
	var tmpout_filename string
	timestamp := time.Now().Format("2006-01-02 15:04:05")

	//Perform read operation.
	reply_buff := prep_rstrct.Cli_obj.Read(prep_rstrct.Zomato_data, prep_rstrct.Rncui, &reply_len)
	if reply_buff == nil {
		fmt.Println("Read request failed !!")
		strdata := zomato_app_info{
			Raft_uuid:   raft_uuid_go,
			Client_uuid: client_uuid_go,
			Leader_uuid: prep_rstrct.Leader_uuid,
			Operation:   prep_rstrct.Operation,
			Status:      -1,
			Timestamp:   timestamp,
			Data:        nil,
		}
		//Dump structure into json.
		tmpout_filename = strdata.dump_into_json(prep_rstrct.Outfile_uuid)
	} else {
		read_data := &zomatoapplib.Zomato_Data{}
		prep_rstrct.Cli_obj.Decode(reply_buff, read_data, reply_len)

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
		fill_data_into_map(read_req_mp, prep_rstrct.Rncui)
		strdata := zomato_app_info{
			Raft_uuid:   raft_uuid_go,
			Client_uuid: client_uuid_go,
			Leader_uuid: prep_rstrct.Leader_uuid,
			Operation:   prep_rstrct.Operation,
			Status:      0,
			Timestamp:   timestamp,
			Data:        data,
		}
		//Dump structure into json.
		tmpout_filename = strdata.dump_into_json(prep_rstrct.Outfile_uuid)
	}
	C.free(reply_buff)
	prep_rstrct.Outfilename = tmpout_filename
}

func (prep_rstrct *ReadOne) Complete() {
	//Copy contents in json outfile.
	copy_to_outfile(prep_rstrct.Outfilename, prep_rstrct.Json_filename)

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

//Method to copy temporary json file into output json file.
func copy_to_outfile(tmp_outfile_name, jsonfilename string) {

	//Prepare json output filepath.
	json_outf := json_outfilepath + "/" + jsonfilename + ".json"
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
			zci = &WriteOne{
				Args:         ops_split,
				Leader_uuid:  leader_uuid,
				Outfile_uuid: outf_uuid,
				Cli_obj:      cli_obj,
			}
			zci.Prepare()
			zci.Exec()
			zci.Complete()
		case "ReadOne":
			zci = &ReadOne{
				Key:           ops_split[1],
				Rncui:         ops_split[2],
				Json_filename: ops_split[3],
				Operation:     ops,
				Leader_uuid:   leader_uuid,
				Outfile_uuid:  outf_uuid,
				Cli_obj:       cli_obj,
			}
			zci.Prepare()
			zci.Exec()
			zci.Complete()
		case "exit":
			os.Exit(0)
		case "default":
			fmt.Println("Enter valid operation: (WriteOne/ WriteMulti/ ReadOne/ ReadMulti/ get_leader /exit)")
		}
	}

}
