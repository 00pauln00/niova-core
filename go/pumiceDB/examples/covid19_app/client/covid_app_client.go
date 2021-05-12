package main

import (
	"bufio"
	"covidapplib/lib"
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
)

/*
#include <stdlib.h>
*/
import "C"

var (
	raft_uuid_go      string
	peer_uuid_go      string
	json_outfile_path string
	leader_uuid       string
	outfile_name      string
	temp_uuid         string
	tmp_outfile       string
	read_data_map     map[string]map[string]string
	write_data_map    map[string]map[string]string
	operation         string
	key_rncui_map     map[string]string
)

/*
 Structure to create json outfile
*/
type covid_app_read struct {
	Raft_uuid   string
	Client_uuid string
	Leader_uuid string
	Operation   string
	Status      int
	Timestamp   string
	Data        map[string]map[string]string
}

type covid_app_write struct {
	Raft_uuid   string
	Client_uuid string
	Leader_uuid string
	Operation   string
	Timestamp   string
	Data        map[string]map[string]string
}

//Function to write write_data_map into map.
func write_reqdata_into_map(key string, status int, app_uuid_str string) {

	status_str := strconv.Itoa(status)
	write_mp := map[string]string{
		"Key":    key,
		"Status": status_str,
	}
	write_data_map[app_uuid_str] = write_mp
}

//Method to dump zomato_app_write structure into json file.
func (covid_out *covid_app_write) dump_writereq_into_json() {

	//Prepare path for temporary json file.
	tmp_outfile = json_outfile_path + "/" + temp_uuid + ".json"

	fmt.Println("tmp outfile", tmp_outfile)

	file, _ := json.MarshalIndent(covid_out, "", "\t")
	_ = ioutil.WriteFile(tmp_outfile, file, 0644)
}

/*
 This function store json output to a map.
*/
func write_output_into_map(read_data *CovidAppLib.Covid_app, read_rncui string) {

	total_vaccinations_int := strconv.Itoa(int(read_data.Total_vaccinations))
	people_vaccinated_int := strconv.Itoa(int(read_data.People_vaccinated))

	mp := map[string]string{
		"Location":           read_data.Location,
		"Iso_code":           read_data.Iso_code,
		"Total_vaccinations": total_vaccinations_int,
		"People_vaccinated":  people_vaccinated_int,
	}

	read_data_map[read_rncui] = mp
}

//Method to dump output map into json file.
func (struct_out *covid_app_read) dump_into_json() {

	file, _ := json.MarshalIndent(struct_out, "", "\t")

	read_outfile := json_outfile_path + "/" + outfile_name + ".json"
	_ = ioutil.WriteFile(read_outfile, file, 0644)
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

//read cmdline input.
func get_cmdline_input(input []string) []string {

	//Read the key from console
	key := bufio.NewReader(os.Stdin)

	key_text, _ := key.ReadString('\n')

	// convert CRLF to LF
	key_text = strings.Replace(key_text, "\n", "", -1)
	input = strings.Split(key_text, "#")

	return input
}

//parse csv file.
func parse_csv_file(filename string) (fp *csv.Reader) {

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
	fp = csv.NewReader(csvfile)

	return fp
}

func copy_tmp_file_to_json_file() {

	//Prepare json output filepath.
	json_outf := json_outfile_path + "/" + outfile_name + ".json"

	fmt.Println("main outfile", json_outf)
	//Create output json file.
	os.Create(json_outf)

	//Copy temporary json file into output json file.
	_, err := exec.Command("cp", tmp_outfile, json_outf).Output()
	if err != nil {
		fmt.Printf("%s", err)
	}

	//Remove temporary outfile after copying into json outfile.
	e := os.Remove(tmp_outfile)
	if e != nil {
		log.Fatal(e)
	}

}

//write operation for csv file parsing.
func covidData_write_by_csvfile(cli_obj *PumiceDBClient.PmdbClientObj, filename string) {

	//call function to parse csv file.
	fp := parse_csv_file(filename)

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

		//Call the function to get rncui.
		rncui := get_rncui_for_csvfile(key_rncui_map, &covid_wr_struct)

		//get length of struct size
		length_wr_struct := cli_obj.GetSize(covid_wr_struct)
		fmt.Println("Length of the structure: ", length_wr_struct)

		fmt.Println("Structure Data:", covid_wr_struct)

		//Get leader-uuid.
		leader_uuid = cli_obj.GetLeader()

		//Write the key-value to pumicedb
		rc := cli_obj.Write(covid_wr_struct, rncui)

		if rc != 0 {
			fmt.Println("Pmdb Write failed.")
			write_strdata := covid_app_write{
				Raft_uuid:   raft_uuid_go,
				Client_uuid: peer_uuid_go,
				Leader_uuid: leader_uuid,
				Operation:   operation,
				Timestamp:   timestamp,
				Data:        write_data_map,
			}

			write_reqdata_into_map(covid_wr_struct.Location, rc, rncui)
			//Dump structure into json.
			write_strdata.dump_writereq_into_json()

		} else {
			fmt.Println("Pmdb Write successful!")
			write_strdata := covid_app_write{
				Raft_uuid:   raft_uuid_go,
				Client_uuid: peer_uuid_go,
				Leader_uuid: leader_uuid,
				Operation:   operation,
				Timestamp:   timestamp,
				Data:        write_data_map,
			}

			write_reqdata_into_map(covid_wr_struct.Location, rc, rncui)
			//Dump structure into json.
			write_strdata.dump_writereq_into_json()
		}
	}
}

//function to write covidData.
func covidData_write(cli_obj *PumiceDBClient.PmdbClientObj,
	pass_input_struct *CovidAppLib.Covid_app, rncui string) {

	//Get the actual size of the structure.
	len_wr_struct := cli_obj.GetSize(pass_input_struct)
	fmt.Println("Length of the structure: ", len_wr_struct)

	//Get timestamp.
	timestamp := time.Now().Format("2006-01-02 15:04:05")

	//Get leader-uuid.
	leader_uuid = cli_obj.GetLeader()

	//Perform write operation.
	rc := cli_obj.Write(pass_input_struct, rncui)

	if rc != 0 {
		fmt.Println("Pmdb Write failed.")
		write_strdata := covid_app_write{
			Raft_uuid:   raft_uuid_go,
			Client_uuid: peer_uuid_go,
			Leader_uuid: leader_uuid,
			Operation:   operation,
			Timestamp:   timestamp,
			Data:        write_data_map,
		}

		write_reqdata_into_map(pass_input_struct.Location, rc, rncui)
		//Dump structure into json.
		write_strdata.dump_writereq_into_json()

		//Copy temporary json file into json outfile.
		copy_tmp_file_to_json_file()

	} else {
		fmt.Println("Pmdb Write successful!")
		write_strdata := covid_app_write{
			Raft_uuid:   raft_uuid_go,
			Client_uuid: peer_uuid_go,
			Leader_uuid: leader_uuid,
			Operation:   operation,
			Timestamp:   timestamp,
			Data:        write_data_map,
		}

		write_reqdata_into_map(pass_input_struct.Location, rc, rncui)
		//Dump structure into json.
		write_strdata.dump_writereq_into_json()

		//Copy temporary json file into json outfile.
		copy_tmp_file_to_json_file()
	}
}

//write operation by passing cmdline data.
func write_covidData_key_value_from_cmdline(cli_obj *PumiceDBClient.PmdbClientObj, input []string) {

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
	covidData_write(cli_obj, &pass_input_struct, get_rncui)
}

//Read Data.
func read_covidData(cli_obj *PumiceDBClient.PmdbClientObj,
	covid_rd_struct *CovidAppLib.Covid_app,
	rd_rncui string) {

	length_rd_struct := cli_obj.GetSize(covid_rd_struct)

	//Print all values which passed to Read()
	fmt.Println("Covid Structure for read:", covid_rd_struct)
	fmt.Println("Length of read:", length_rd_struct)

	timestamp := time.Now().Format("2006-01-02 15:04:05")

	//Get leader-uuid.
	leader_uuid = cli_obj.GetLeader()

	var reply_size int64

	//read operation
	reply_buff := cli_obj.Read(covid_rd_struct, rd_rncui,
		&reply_size)

	if reply_buff == nil {

		rc := -1

		fmt.Println("Read request failed !!")

		strdata := covid_app_read{
			Raft_uuid:   raft_uuid_go,
			Client_uuid: peer_uuid_go,
			Leader_uuid: leader_uuid,
			Operation:   operation,
			Status:      rc,
			Timestamp:   timestamp,
			Data:        nil,
		}

		strdata.dump_into_json()

	} else {
		rc := 0

		struct_op := &CovidAppLib.Covid_app{}
		cli_obj.Decode(reply_buff, struct_op, reply_size)

		fmt.Println("Result of the read request is: ", struct_op)

		write_output_into_map(struct_op, rd_rncui)

		strdata := covid_app_read{
			Raft_uuid:   raft_uuid_go,
			Client_uuid: peer_uuid_go,
			Leader_uuid: leader_uuid,
			Operation:   operation,
			Status:      rc,
			Timestamp:   timestamp,
			Data:        read_data_map,
		}

		strdata.dump_into_json()
	}
	// Application should free the this reply_buff which is allocate by pmdb lib
	C.free(reply_buff)
}

func read_covidData_key_value_from_cmdline(cli_obj *PumiceDBClient.PmdbClientObj, input []string) {

	key := input[1]
	rd_rncui := input[2]

	pass_rd_struct := CovidAppLib.Covid_app{
		Location: key,
	}

	fmt.Println("rncui: ", rd_rncui)
	fmt.Println("key: ", key)

	//call function to read cmdline data.
	read_covidData(cli_obj, &pass_rd_struct, rd_rncui)
}

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

	//Create and Initialize the map.
	key_rncui_map = make(map[string]string)

	fmt.Println("=================Format to pass write-read entries================")
	fmt.Println("Single write format ==> WriteOne#Rncui#Key#Val0#Val1#Val2#outfile_name")
	fmt.Println("Single read format ==> ReadOne#Key#Rncui#outfile_name")
	fmt.Println("Multiple write format ==> WriteMulti#csvfile.csv#outfile_name")
	fmt.Println("Multiple read format ==> ReadMulti#outfile_name")
	fmt.Println("Get Leader format ==> get_leader#outfile_name")

	for {

		fmt.Print("Enter operation(WriteOne/ WriteMulti/ ReadOne/ ReadMulti/ get_leader/ exit): ")

		//Create and Initialize map for write-read oufile.
		read_data_map = make(map[string]map[string]string)
		write_data_map = make(map[string]map[string]string)

		var str []string

		//Pass input string from console.
		input := get_cmdline_input(str)

		operation = input[0]

		if operation == "WriteMulti" {

			temp_uuid = uuid.NewV4().String()
			csv_filepath := input[1]
			outfile_name = input[2]
			//Write operation using csv file parsing.
			covidData_write_by_csvfile(cli_obj, csv_filepath)

			//Copy temporary json file into json outfile.
			copy_tmp_file_to_json_file()

		} else if operation == "WriteOne" {

			outfile_name = input[6]
			temp_uuid = uuid.NewV4().String()
			//call function to write covid19 data from cmdline.
			write_covidData_key_value_from_cmdline(cli_obj, input)

		} else if operation == "ReadOne" {

			outfile_name = input[3]
			//call function to read cmdline data.
			read_covidData_key_value_from_cmdline(cli_obj, input)

		} else if operation == "ReadMulti" {
			outfile_name = input[1]
			//Iterate over map to read all data from csv file.
			for key, rd_rncui := range key_rncui_map {
				fmt.Println(key, " ", rd_rncui)
				covid_rd_struct := CovidAppLib.Covid_app{
					Location: key,
				}
				//call function to read csv file.
				read_covidData(cli_obj, &covid_rd_struct, rd_rncui)
			}

		} else if operation == "get_leader" {
			outfile_name = input[1]
			leader_uuid = cli_obj.GetLeader()
			fmt.Println("Leader uuid is: ", leader_uuid)

			//Get timestamp.
			timestamp := time.Now().Format("2006-01-02 15:04:05")

			get_leader_uuid_struct := covid_app_read{
				Raft_uuid:   raft_uuid_go,
				Client_uuid: peer_uuid_go,
				Leader_uuid: leader_uuid,
				Operation:   operation,
				Timestamp:   timestamp,
			}

			//Dump structure into json.
			get_leader_uuid_struct.dump_into_json()

		} else if operation == "exit" {
			os.Exit(0)
		} else {
			fmt.Println("\nEnter valid operation: WriteOne/ReadOne/WriteMulti/ReadMulti/get_leader/exit")
		}
	}
}
