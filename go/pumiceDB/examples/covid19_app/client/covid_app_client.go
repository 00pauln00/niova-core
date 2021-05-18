package main

import (
	"bufio"
	"covidapplib/lib"
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
)

/*
#include <stdlib.h>
*/
import "C"

var (
	raft_uuid_go      string
	peer_uuid_go      string
	json_outfile_path string
	data	          map[string]map[string]string
	key_rncui_map     map[string]string
)

/*
 Structure to create json outfile
*/
type CovidVaxData struct {
	Raft_uuid   string
	Client_uuid string
	Leader_uuid string
	Operation   string
	Status      int
	Timestamp   string
	Data        map[string]map[string]string
	CV_obj PumiceDBClient.CommonVars
}

//Function to write write_data into map.
func fill_data_into_map(mp map[string]string, rncui string) {

	//Fill data into outer map.
	data[rncui] = mp
}

//Method to dump output map into json file.
func (struct_out *CovidVaxData) dump_into_json() string {

	//Prepare path for temporary json file.
	tmp_outfile := json_outfile_path + "/" + struct_out.CV_obj.Outfile_uuid + ".json"
	file, _ := json.MarshalIndent(struct_out, "", "\t")
	_ = ioutil.WriteFile(tmp_outfile, file, 0644)

	return tmp_outfile
}

/*This function stores rncui for all csv file
  data into a key_rncui_map and returns that rncui.
*/
func get_rncui_for_csvfile(key_rncui_map map[string]string, covid_wr_struct *CovidAppLib.Covid_app) string {

	//Generate app_uuid.
	app_uuid := uuid.NewV4().String()

	//Create rncui string.
	rncui := app_uuid + ":0:0:0:0"

	key_rncui_map[covid_wr_struct.Location] = rncui

	return rncui
}

func (struct_out *CovidVaxData) copy_tmp_file_to_json_file(tmp_outfile_name string) {

	//Prepare json output filepath.
	json_outf := json_outfile_path + "/" + struct_out.CV_obj.Json_filename + ".json"

	fmt.Println("main outfile", json_outf)
	//Create output json file.
	os.Create(json_outf)

	//Copy temporary json file into output json file.
	_, err := exec.Command("cp", tmp_outfile_name, json_outf).Output()
	if err != nil {
		fmt.Printf("%s", err)
	}

	//Remove temporary outfile after copying into json outfile.
	e := os.Remove(tmp_outfile_name)
	if e != nil {
		log.Fatal(e)
	}

}

//write operation for csv file parsing.
func (struct_out *CovidVaxData) WriteMultiStruct(filename string) string {

	var tmp_outfile string
	//call function to parse csv file.
	fp := struct_out.CV_obj.ParseFile(filename)

	//Get timestamp.
	timestamp := time.Now().Format("2006-01-02 15:04:05")

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
			Location:           record[0],
			Iso_code:           record[1],
			Total_vaccinations: Total_vaccinations_int,
			People_vaccinated:  People_vaccinated_int,
		}

		//Call the function to get rncui.`
		rncui := get_rncui_for_csvfile(key_rncui_map, &covid_wr_struct)

		//get length of struct size
		length_wr_struct := struct_out.CV_obj.Cli_obj.GetSize(covid_wr_struct)
		fmt.Println("Length of the structure: ", length_wr_struct)

		fmt.Println("Structure Data:", covid_wr_struct)

		//Get leader-uuid.
		//leader_uuid = gopmdb_appstruct.cli_obj.GetLeader()

		//Write the key-value to pumicedb
		rc := struct_out.CV_obj.Cli_obj.Write(covid_wr_struct, rncui)

		if rc != 0 {
			fmt.Println("Pmdb Write failed.")

			write_strdata := CovidVaxData{
				Raft_uuid:   raft_uuid_go,
				Client_uuid: peer_uuid_go,
				Leader_uuid: struct_out.Leader_uuid,
	                        Operation:   struct_out.Operation,
				Timestamp:   timestamp,
				Data:        data,
			}

			status_str := strconv.Itoa(int(rc))
			write_mp := map[string]string{
				"Key":    covid_wr_struct.Location,
				"Status": status_str,
			}

			//Fill write request data into a map.
			fill_data_into_map(write_mp, rncui)

			//Dump structure into json.
                        tmp_outfile = write_strdata.dump_into_json()

		} else {
			fmt.Println("Pmdb Write successful!")
			write_strdata := CovidVaxData{
				Raft_uuid:   raft_uuid_go,
				Client_uuid: peer_uuid_go,
				Leader_uuid: struct_out.Leader_uuid,
	                        Operation:   struct_out.Operation,
				Timestamp:   timestamp,
				Data:        data,
			}

			status_str := strconv.Itoa(int(rc))
                        write_mp := map[string]string{
                                "Key":    covid_wr_struct.Location,
                                "Status": status_str,
                        }

                        //Fill write request data into a map.
                        fill_data_into_map(write_mp, rncui)

			//Dump structure into json.
			tmp_outfile = write_strdata.dump_into_json()

		}
	}
	return tmp_outfile
}

//function to write covidData.
func (struct_out *CovidVaxData) WriteOneStruct(pass_input_struct *CovidAppLib.Covid_app,
	rncui string) {

	//Get the actual size of the structure.
	len_wr_struct := struct_out.CV_obj.Cli_obj.GetSize(pass_input_struct)
	fmt.Println("Length of the structure: ", len_wr_struct)

	//Get timestamp.
	timestamp := time.Now().Format("2006-01-02 15:04:05")

	//Get leader-uuid.
	//leader_uuid = cli_obj.GetLeader()

	//Perform write operation.
	rc := struct_out.CV_obj.Cli_obj.Write(pass_input_struct, rncui)

	if rc != 0 {
		fmt.Println("Pmdb Write failed.")

		write_strdata := CovidVaxData{
			Raft_uuid:   raft_uuid_go,
			Client_uuid: peer_uuid_go,
			Leader_uuid: struct_out.Leader_uuid,
			Operation:   struct_out.Operation,
			Timestamp:   timestamp,
			Data:        data,
		}

		status_str := strconv.Itoa(int(rc))
		write_mp := map[string]string{
			"Key":    pass_input_struct.Location,
			"Status": status_str,
		}

		//Fill write request data into a map.
		fill_data_into_map(write_mp, rncui)

		//Dump structure into json.
                tmp_outfile := write_strdata.dump_into_json()

                //Copy temporary json file into json outfile.
                struct_out.copy_tmp_file_to_json_file(tmp_outfile)

	} else {
		fmt.Println("Pmdb Write successful!")
		write_strdata := CovidVaxData{
			Raft_uuid:   raft_uuid_go,
			Client_uuid: peer_uuid_go,
			Leader_uuid: struct_out.Leader_uuid,
			Operation:   struct_out.Operation,
			Timestamp:   timestamp,
			Data:        data,
		}

		status_str := strconv.Itoa(int(rc))
                write_mp := map[string]string{
                        "Key":    pass_input_struct.Location,
                        "Status": status_str,
                }

                //Fill write request data into a map.
                fill_data_into_map(write_mp, rncui)

		//Dump structure into json.
		tmp_outfile := write_strdata.dump_into_json()

		//Copy temporary json file into json outfile.
		struct_out.copy_tmp_file_to_json_file(tmp_outfile)
	}
}

//write operation by passing cmdline data.
func (struct_out *CovidVaxData) WriteOneExec(input []string) {

	input := GetInput()

	get_rncui := input[1]
	input_key := input[2]
	field0_val := input[3]
	field1_val := input[4]
	field2_val := input[5]
	//outfile_name = input[6]

	//typecast the data type to int
	field1_int, _ := strconv.ParseInt(field1_val, 10, 64)
	field2_int, _ := strconv.ParseInt(field2_val, 10, 64)

	/*
		Prepare the structure from values passed by user.
		fill the struture
	*/
	pass_input_struct := CovidAppLib.Covid_app{
		Location:           input_key,
		Iso_code:           field0_val,
		Total_vaccinations: field1_int,
		People_vaccinated:  field2_int,
	}

	fmt.Println("rncui: ", get_rncui)
	fmt.Println("key: ", pass_input_struct.Location)
	fmt.Println("value0: ", pass_input_struct.Iso_code)
	fmt.Println("value1: ", pass_input_struct.Total_vaccinations)
	fmt.Println("value2: ", pass_input_struct.People_vaccinated)

	//call function to write from cmdline.
	struct_out.WriteOneStruct(&pass_input_struct, get_rncui)
}

//Read Data.
func (struct_out *CovidVaxData) ReadStruct(cli_obj *PumiceDBClient.PmdbClientObj,
	covid_rd_struct *CovidAppLib.Covid_app,
	rd_rncui string) {

	var tmp_outfile string

	length_rd_struct := struct_out.CV_obj.Cli_obj.GetSize(covid_rd_struct)

	//Print all values which passed to Read()
	fmt.Println("Covid Structure for read:", covid_rd_struct)
	fmt.Println("Length of read:", length_rd_struct)

	timestamp := time.Now().Format("2006-01-02 15:04:05")

	//Get leader-uuid.
	leader_uuid = cli_obj.GetLeader()

	var reply_size int64

	//read operation
	reply_buff := struct_out.CV_obj.Cli_obj..Read(covid_rd_struct, rd_rncui,
		&reply_size)

	if reply_buff == nil {

		rc := -1

		fmt.Println("Read request failed !!")

		strdata := CovidVaxData{
			Raft_uuid:   raft_uuid_go,
			Client_uuid: peer_uuid_go,
			Leader_uuid: leader_uuid,
			Operation:   operation,
			Status:      rc,
			Timestamp:   timestamp,
			Data:        nil,
		}

		//Dump structure into json.
                tmp_outfile := strdata.dump_into_json()

                //Copy temporary json file into json outfile.
                struct_out.copy_tmp_file_to_json_file(tmp_outfile)


	} else {
		rc := 0

		struct_op := &CovidAppLib.Covid_app{}
		cli_obj.Decode(reply_buff, struct_op, reply_size)

		fmt.Println("Result of the read request is: ", struct_op)

		strdata := CovidVaxData{
			Raft_uuid:   raft_uuid_go,
			Client_uuid: peer_uuid_go,
			Leader_uuid: leader_uuid,
			Operation:   operation,
			Status:      rc,
			Timestamp:   timestamp,
			Data:        data,
		}

		total_vaccinations_int := strconv.Itoa(int(struct_op.Total_vaccinations))
                people_vaccinated_int := strconv.Itoa(int(struct_op.People_vaccinated))

                read_map := map[string]string{
                        "Location":           struct_op.Location,
                        "Iso_code":           struct_op.Iso_code,
                        "Total_vaccinations": total_vaccinations_int,
                        "People_vaccinated":  people_vaccinated_int,
                }

		//Fill write request data into a map.
                fill_data_into_map(read_map, rd_rncui)

		//Dump structure into json.
                tmp_outfile := write_strdata.dump_into_json()

                //Copy temporary json file into json outfile.
                struct_out.copy_tmp_file_to_json_file(tmp_outfile)


	}
	// Application should free the this reply_buff which is allocate by pmdb lib
	C.free(reply_buff)
}

func ReadOneStruct(cli_obj *PumiceDBClient.PmdbClientObj, input []string) {

	key := input[1]
	rd_rncui := input[2]

	pass_rd_struct := CovidAppLib.Covid_app{
		Location: key,
	}

	fmt.Println("rncui: ", rd_rncui)
	fmt.Println("key: ", key)

	//call function to read cmdline data.
	ReadStruct(cli_obj, &pass_rd_struct, rd_rncui)
}
*/

func pmdb_dict_app_getopts() {

	flag.StringVar(&raft_uuid_go, "r", "NULL", "raft uuid")
	flag.StringVar(&peer_uuid_go, "u", "NULL", "peer uuid")
	flag.StringVar(&json_outfile_path, "l", "NULL", "json outfile path")

	flag.Parse()

	fmt.Println("Raft UUID: ", raft_uuid_go)
	fmt.Println("Peer UUID: ", peer_uuid_go)
	fmt.Println("Outfile Path: ", json_outfile_path)
}

func main() {

	//Print help message.
	if len(os.Args) == 1 || os.Args[1] == "-help" || os.Args[1] == "-h" {
		fmt.Println("You need to pass the following arguments:")
		fmt.Println("Positional Arguments: \n		'-r' - RAFT UUID \n		'-u' - PEER UUID \n		'-l' - Outfile path")
		fmt.Println("Optional Arguments: \n		-h, -help")
		fmt.Println("Pass arguments in this format: \n		./covid_app_client -r RAFT UUID -u PEER UUID -l Log file path")
		os.Exit(0)
	}

	//Parse the cmdline parameter
	pmdb_dict_app_getopts()

	// sleep for 2sec.
	fmt.Println("Wait for 2 sec to start client")
	//Create new client object.
	cli_obj := PumiceDBClient.PmdbClientNew(raft_uuid_go, peer_uuid_go)
	if cli_obj == nil {
		return
	}

	//Start the client
	cli_obj.Start()
	defer cli_obj.Stop()

	fmt.Println("=================Format to pass write-read entries================")
	fmt.Println("Single write format ==> WriteOne#Rncui#Key#Val0#Val1#Val2#outfile_name")
	fmt.Println("Single read format ==> ReadOne#Key#Rncui#outfile_name")
	fmt.Println("Multiple write format ==> WriteMulti#csvfile.csv#outfile_name")
	fmt.Println("Multiple read format ==> ReadMulti#outfile_name")
	fmt.Println("Get Leader format ==> get_leader#outfile_name")

	for {

		fmt.Print("Enter operation(WriteOne/ WriteMulti/ ReadOne/ ReadMulti/ get_leader/ exit): ")

		//Create and Initialize map for write-read oufile.
		data = make(map[string]map[string]string)

		//Create and Initialize the map.
		key_rncui_map = make(map[string]string)

		//Create temporary UUID
		outfile_uuid := uuid.NewV4().String()
		//Get Leader UUID by calling GetLeader().
		leader_uuid := cli_obj.GetLeader()

		operation := input[0]

		switch operation {

		case "WriteMulti":
			csv_filename := input[1]
			outfile_name := input[2]
			cvd := CovidVaxData{
				Raft_uuid:   raft_uuid_go,
				Client_uuid: peer_uuid_go,
				Leader_uuid: leader_uuid,
				Operation:   operation,
				CV_obj: PumiceDBClient.CommonVars{
					Outfile_uuid:  outfile_uuid,
					Json_filename: outfile_name,
				},
			}

			//Write operation using csv file parsing.
			tmp_outfile := cvd.WriteMultiStruct(csv_filename)

			//Copy temporary json file into json outfile.
			cvd.copy_tmp_file_to_json_file(tmp_outfile)

		case "WriteOne":
			outfile_name := input[6]
			cvd := CovidVaxData{
                                Raft_uuid:   raft_uuid_go,
                                Client_uuid: peer_uuid_go,
                                Leader_uuid: leader_uuid,
                                Operation:   operation,
                                CV_obj: PumiceDBClient.CommonVars{
                                        Outfile_uuid:  outfile_uuid,
                                        json_filename: outfile_name,
                                },
                        }

			//call function to write covid19 data from cmdline.
			cvd.WriteOneExec(input)

		case "ReadOne":
			outfile_name = input[3]
			//call function to read cmdline data.
			ReadOneStruct(cli_obj, input)

		case "ReadMulti":
			outfile_name = input[1]
			//Iterate over map to read all data from csv file.
			for key, rd_rncui := range key_rncui_map {
				fmt.Println(key, " ", rd_rncui)
				covid_rd_struct := CovidAppLib.Covid_app{
					Location: key,
				}
				//call function to read csv file.
				ReadStruct(cli_obj, &covid_rd_struct, rd_rncui)
			}

		case "get_leader":
			outfile_name = input[1]
			fmt.Println("Leader uuid is: ", leader_uuid)

			//Get timestamp.
			timestamp := time.Now().Format("2006-01-02 15:04:05")

			get_leader_uuid_struct := CovidVaxData{
				Raft_uuid:   raft_uuid_go,
				Client_uuid: peer_uuid_go,
				Leader_uuid: leader_uuid,
				Operation:   operation,
				Timestamp:   timestamp,
			}

			//Dump structure into json.
			get_leader_uuid_struct.dump_into_json()

		case "exit":
			os.Exit(0)
		default:
			fmt.Println("\nEnter valid operation: WriteOne/ReadOne/WriteMulti/ReadMulti/get_leader/exit")
		}
	}
}
