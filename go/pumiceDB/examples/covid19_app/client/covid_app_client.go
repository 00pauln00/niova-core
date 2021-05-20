package main

import (
	"bufio"
	"covidapplib/lib"
	"encoding/json"
	"flag"
	"fmt"
	"errors"
	"github.com/satori/go.uuid"
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
	data              map[string]map[string]string
	key_rncui_map     map[string]string
)

//Interface for WROne
type WORO_iface interface {
	Prepare([]string) error
	Exec([]string) error
	Complete() error
}

/*
 Structure for Common items from WriteOne
 and ReadOne struct
*/
type WROne struct {
	Leader_uuid   string
	Outfile_uuid  string
	Outfile_name  string
	Json_filename string
	Covid_data    *CovidAppLib.Covid_app
	Cli_obj       *PumiceDBClient.PmdbClientObj
}

/*
 Structure for WriteOne operation
*/
type WriteOne struct {
	CommonObj *WROne
}

/*
 Structure for ReadOne operation
*/
type ReadOne struct {
	CommonObj *WROne
}

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
}

//Function to write write_data into map.
func fill_data_into_map(mp map[string]string, rncui string) {

	//Fill data into outer map.
	data[rncui] = mp
}

//Method to dump CovidVaxData structure into json file.
func (struct_out *CovidVaxData) dump_into_json(outfile_uuid string) string {

	//Prepare path for temporary json file.
	temp_outfile_name := json_outfile_path + "/" + outfile_uuid + ".json"
	file, _ := json.MarshalIndent(struct_out, "", "\t")
	_ = ioutil.WriteFile(temp_outfile_name, file, 0644)

	return temp_outfile_name
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

func (wr_data *CovidVaxData) Fill_WStruct(input []string, Wr *WriteOne) {

	//Get timestamp.
	timestamp := time.Now().Format("2006-01-02 15:04:05")

	wr_data = &CovidVaxData{
		Raft_uuid:   raft_uuid_go,
		Client_uuid: peer_uuid_go,
		Leader_uuid: Wr.CommonObj.Leader_uuid,
		Operation:   input[0],
		Timestamp:   timestamp,
		Data:        data,
	}
	status_str := strconv.Itoa(int(wr_data.Status))
	write_mp := map[string]string{
		"Key":    Wr.CommonObj.Covid_data.Location,
		"Status": status_str,
	}

	//Fill write request data into a map.
	fill_data_into_map(write_mp, input[1])

	//Dump structure into json.
	Wr.CommonObj.Outfile_name = wr_data.dump_into_json(Wr.CommonObj.Outfile_uuid)
}

func (rd_data *CovidVaxData) Fill_RStruct(input []string, Rd *ReadOne) {

	//Get timestamp.
	timestamp := time.Now().Format("2006-01-02 15:04:05")

	rd_data = &CovidVaxData{
		Raft_uuid:   raft_uuid_go,
		Client_uuid: peer_uuid_go,
		Leader_uuid: Rd.CommonObj.Leader_uuid,
		Operation:   input[0],
		Timestamp:   timestamp,
		Data:        data,
	}

	//Dump structure into json.
	Rd.CommonObj.Outfile_name = rd_data.dump_into_json(Rd.CommonObj.Outfile_uuid)
}

func copy_tmp_file_to_json_file(temp_outfile_name string, json_filename string) error {

	var cp_err error
	//Prepare json output filepath.
	json_outf := json_outfile_path + "/" + json_filename + ".json"

	fmt.println("main outfile", json_outf)
	//Create output json file.
	os.Create(json_outf)

	//Copy temporary json file into output json file.
	_, err := exec.Command("cp", temp_outfile_name, json_outf).Output()
	if err != nil {
		fmt.Printf("%s", err)
		cp_err = err
	} else {
		cp_err = nil
	}

	//Remove temporary outfile after copying into json outfile.
	e := os.Remove(temp_outfile_name)
	if e != nil {
		log.Fatal(e)
	}
	return cp_err
}

func (wr_obj *WriteOne) Prepare(input []string) error {

	var err error
	input_key := input[2]
	field0_val := input[3]
	field1_val := input[4]
	field2_val := input[5]

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
	wr_obj.CommonObj.Covid_data = &pass_input_struct
	if wr_obj.CommonObj.Covid_data == nil {
		err = errors.New("Prepare() method failed for WriteOne.")
	}
	return err
}

func (wr_obj *WriteOne) Exec(input []string) error {

	var err_msg error
	rncui := input[1]
	//Get the actual size of the structure.
	len_wr_struct := wr_obj.CommonObj.Cli_obj.GetSize(wr_obj.CommonObj.Covid_data)
	fmt.Println("Length of the structure: ", len_wr_struct)

	//Perform write operation.
	rc := wr_obj.CommonObj.Cli_obj.Write(wr_obj.CommonObj.Covid_data, rncui)

	fill_wrdata := &CovidVaxData{Status: rc}
	if rc != 0 {
		err_msg = errors.New("Exec() method failed for WriteOne.")
		fill_wrdata.Fill_WStruct(input, wr_obj)
	} else {
		fmt.Println("Pmdb Write successful!")
		err_msg = nil
		fill_wrdata.Fill_WStruct(input, wr_obj)
	}
	return err_msg
}

func (wr_obj *WriteOne) Complete() error {

	var c_err error
	//Copy temporary json file into json outfile.
	err := copy_tmp_file_to_json_file(wr_obj.CommonObj.Outfile_name,
					wr_obj.CommonObj.Json_filename)
	if err != nil {
		c_err = errors.New("Complete() method failed for WriteOne.")
	}
	return c_err
}

func (rd_obj *ReadOne) Prepare(input []string) error {

	var err error
	key := input[1]

	pass_rd_struct := CovidAppLib.Covid_app{
		Location: key,
	}

	rd_obj.CommonObj.Covid_data = &pass_rd_struct
	if rd_obj.CommonObj.Covid_data == nil {
                err = errors.New("Prepare() method failed for ReadOne.")
        }
        return err
}

func (rd_obj *ReadOne) Exec(input []string) error {

	var err error
	rd_rncui := input[2]

	length_rd_struct := rd_obj.CommonObj.Cli_obj.GetSize(rd_obj.CommonObj.Covid_data)

	//Print all values which passed to Read()
	fmt.Println("Covid Structure for read:", rd_obj.CommonObj.Covid_data)
	fmt.Println("Length of read:", length_rd_struct)

	var reply_size int64

	//read operation
	reply_buff := rd_obj.CommonObj.Cli_obj.Read(rd_obj.CommonObj.Covid_data, rd_rncui,
		&reply_size)

	if reply_buff == nil {

		fmt.Println("Read request failed !!")

		rd_strdata := &CovidVaxData{Status: -1}
		rd_strdata.Fill_RStruct(input, rd_obj)
		err = errors.New("Exec() method failed for ReadOne")
	} else {
		req_struct := &CovidAppLib.Covid_app{}
		rd_obj.CommonObj.Cli_obj.Decode(reply_buff, req_struct, reply_size)
		fmt.Println("Result of the read request is: ", req_struct)

		total_vaccinations_int := strconv.Itoa(int(req_struct.Total_vaccinations))
		people_vaccinated_int := strconv.Itoa(int(req_struct.People_vaccinated))

		read_map := map[string]string{
			"Location":           req_struct.Location,
			"Iso_code":           req_struct.Iso_code,
			"Total_vaccinations": total_vaccinations_int,
			"People_vaccinated":  people_vaccinated_int,
		}
		//Fill write request data into a map.
		fill_data_into_map(read_map, input[2])

		rd_strdata := &CovidVaxData{Status: 0}
		rd_strdata.Fill_RStruct(input, rd_obj)
		err = nil
	}
	// Application should free the this reply_buff which is allocate by pmdb lib
	C.free(reply_buff)
	return err
}

func (rd_obj *ReadOne) Complete() error {

	var c_err error
	//Copy temporary json file into json outfile.
	err := copy_tmp_file_to_json_file(rd_obj.CommonObj.Outfile_name,
				rd_obj.CommonObj.Json_filename)
	if err != nil {
                c_err = errors.New("Complete() method failed for ReadOne.")
        }
        return c_err
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

	fmt.Println("=================Format to pass write-read entries================")
	fmt.Println("Single write format ==> WriteOne#Rncui#Key#Val0#Val1#Val2#outfile_name")
	fmt.Println("Single read format ==> ReadOne#Key#Rncui#outfile_name")
	fmt.Println("Multiple write format ==> WriteMulti#csvfile.csv#outfile_name")
	fmt.Println("Multiple read format ==> ReadMulti#outfile_name")
	fmt.Println("Get Leader format ==> get_leader#outfile_name")

	for {

		fmt.Print("Enter operation(WROne/ WriteMulti/ WROne/ ReadMulti/ get_leader/ exit): ")

		//Create and Initialize map for write-read oufile.
		data = make(map[string]map[string]string)

		//Create and Initialize the map.
		key_rncui_map = make(map[string]string)

		//Create temporary UUID
		temp_uuid := uuid.NewV4().String()
		//Get Leader
		leader_uuid := cli_obj.GetLeader()

		var str []string

		//Pass input string from console.
		input := get_cmdline_input(str)

		operation := input[0]

		var wr_iface WORO_iface

		switch operation {

		case "WriteOne":
			outfile_name := input[6]
			wr_iface = &WriteOne{
				CommonObj: &WROne{
					Leader_uuid:   leader_uuid,
					Outfile_uuid:  temp_uuid,
					Json_filename: outfile_name,
					Cli_obj:       cli_obj,
				},
			}
		case "ReadOne":
			outfile_name := input[3]
			wr_iface = &ReadOne{
				CommonObj: &WROne{
					Leader_uuid:   leader_uuid,
					Outfile_uuid:  temp_uuid,
					Json_filename: outfile_name,
					Cli_obj:       cli_obj,
				},
			}

		case "exit":
			os.Exit(0)
		default:
			fmt.Println("\nEnter valid operation: WriteOne/ReadOne/WriteMulti/ReadMulti/get_leader/exit")
		}
		//Call set of methods from interface
		P_err := wr_iface.Prepare(input)
		if P_err != nil {
			log.Fatal("error")
		}
		E_err := wr_iface.Exec(input)
		if E_err != nil {
                        log.Fatal("error")
                }
		C_err := wr_iface.Complete()
                if C_err != nil {
                        log.Fatal("error")
                }
	}
}
