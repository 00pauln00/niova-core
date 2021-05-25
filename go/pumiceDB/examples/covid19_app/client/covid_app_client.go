package main

import (
	"bufio"
	"covidapplib/lib"
	"encoding/csv"
	"encoding/json"
	"errors"
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
	data              map[string]map[string]string
	key_rncui_map     map[string]string
	WriteMulti_map    map[CovidAppLib.Covid_app]string
)

//Interface for RDWR
type RDWR_iface interface {
	Prepare() error
	Exec() error
	Complete() error
}

/*
 Structure for Common items from RDWR operations
*/
type RDWR struct {
	Leader_uuid   string
	Outfile_uuid  string
	Outfile_name  string
	Json_filename string
	Key           string
	Rncui         string
	Input         []string
	Covid_data    *CovidAppLib.Covid_app
	Cli_obj       *PumiceDBClient.PmdbClientObj
}

/*
 Structure for WriteOne operation
*/
type WRONE struct {
	CommonObj RDWR
}

/*
 Structure for ReadOne operation
*/
type RDONE struct {
	CommonObj RDWR
}

/*
 Structure for WriteMulti operation
*/
type WRMUL struct {
	Csvfile   string
	CommonObj RDWR
}

/*
 Structure for ReadMulti operation
*/
type RDMUL struct {
	Multi_rd    []*CovidAppLib.Covid_app
	Multi_rncui []string
	CommonObj   RDWR
}

/*
 Structure for GetLeader operation
*/
type GetLeader struct {
	CommonObj   RDWR
	Leader_data CovidVaxData
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

/*This function stores rncui for all csv file
  data into a key_rncui_map and returns that rncui.
*/
func get_rncui_for_csvfile(key_rncui_map map[string]string,
	covid_wr_struct *CovidAppLib.Covid_app) string {

	//Generate app_uuid.
	app_uuid := uuid.NewV4().String()

	//Create rncui string.
	rncui := app_uuid + ":0:0:0:0"

	key_rncui_map[covid_wr_struct.Location] = rncui

	return rncui
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

func (wr_one *CovidVaxData) Fill_WriteOne(wr *WRONE) {

	//Get timestamp.
	timestamp := time.Now().Format("2006-01-02 15:04:05")

	//fill the value into json structure.
	wr_one.Raft_uuid = raft_uuid_go
	wr_one.Client_uuid = peer_uuid_go
	wr_one.Leader_uuid = wr.CommonObj.Leader_uuid
	wr_one.Operation = wr.CommonObj.Input[0]
	wr_one.Timestamp = timestamp
	wr_one.Data = data

	status_str := strconv.Itoa(int(wr_one.Status))
	write_mp := map[string]string{
		"Key":    wr.CommonObj.Key,
		"Status": status_str,
	}

	//fill write request data into a map.
	fill_data_into_map(write_mp, wr.CommonObj.Rncui)

}

func (rd_one *CovidVaxData) Fill_ReadOne(rd *RDONE) {

	//Get timestamp.
	timestamp := time.Now().Format("2006-01-02 15:04:05")

	//Fill the value into Json structure.
	rd_one.Raft_uuid = raft_uuid_go
	rd_one.Client_uuid = peer_uuid_go
	rd_one.Leader_uuid = rd.CommonObj.Leader_uuid
	rd_one.Operation = rd.CommonObj.Input[0]
	rd_one.Timestamp = timestamp
	rd_one.Data = data
}

func (wr_data *CovidVaxData) Fill_WriteMulti(wm *WRMUL) {

	//get timestamp.
	timestamp := time.Now().Format("2006-01-02 15:04:05")

	//fill the value into json structure.
	wr_data.Raft_uuid = raft_uuid_go
	wr_data.Client_uuid = peer_uuid_go
	wr_data.Leader_uuid = wm.CommonObj.Leader_uuid
	wr_data.Operation = wm.CommonObj.Input[0]
	wr_data.Timestamp = timestamp
	wr_data.Data = data

	status_str := strconv.Itoa(int(wr_data.Status))
	write_mp := map[string]string{
		"Key":    wm.CommonObj.Key,
		"Status": status_str,
	}

	//fill write request data into a map.
	fill_data_into_map(write_mp, wm.CommonObj.Rncui)
}

func (rd_data *CovidVaxData) Fill_ReadMulti(rm *RDMUL) {

	//Get timestamp.
	timestamp := time.Now().Format("2006-01-02 15:04:05")

	//Fill the value into Json structure.
	rd_data.Raft_uuid = raft_uuid_go
	rd_data.Client_uuid = peer_uuid_go
	rd_data.Leader_uuid = rm.CommonObj.Leader_uuid
	rd_data.Operation = rm.CommonObj.Input[0]
	rd_data.Timestamp = timestamp
	rd_data.Data = data
}

func copy_tmp_file_to_json_file(temp_outfile_name string, json_filename string) error {

	var cp_err error
	//Prepare json output filepath.
	json_outf := json_outfile_path + "/" + json_filename + ".json"

	fmt.Println("main outfile", json_outf)
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

func (wr_obj *WRONE) Prepare() error {

	var err error

	field0_val := wr_obj.CommonObj.Input[3]
	field1_val := wr_obj.CommonObj.Input[4]
	field2_val := wr_obj.CommonObj.Input[5]

	//typecast the data type to int
	field1_int, _ := strconv.ParseInt(field1_val, 10, 64)
	field2_int, _ := strconv.ParseInt(field2_val, 10, 64)

	/*
	   Prepare the structure from values passed by user.
	   fill the struture
	*/
	pass_input_struct := CovidAppLib.Covid_app{
		Location:           wr_obj.CommonObj.Key,
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

func (wr_obj *WRONE) Exec() error {

	var err_msg error
	var fill_wrone = &CovidVaxData{}

	//Perform write operation.
	err := wr_obj.CommonObj.Cli_obj.Write(wr_obj.CommonObj.Covid_data,
		wr_obj.CommonObj.Rncui)

	if err != nil {
		err_msg = errors.New("Exec() method failed for WriteOne.")
		fill_wrone.Status = -1
	} else {
		fmt.Println("Pmdb Write successful!")
		fill_wrone.Status = 0
		err_msg = nil
	}
	fill_wrone.Fill_WriteOne(wr_obj)
	//Dump structure into json.
	wr_obj.CommonObj.Outfile_name = fill_wrone.dump_into_json(wr_obj.CommonObj.Outfile_uuid)

	return err_msg
}

func (wr_obj *WRONE) Complete() error {

	var c_err error

	//Copy temporary json file into json outfile.
	err := copy_tmp_file_to_json_file(wr_obj.CommonObj.Outfile_name,
		wr_obj.CommonObj.Json_filename)
	if err != nil {
		c_err = errors.New("Complete() method failed for WriteOne.")
	}
	return c_err
}

func (rd_obj *RDONE) Prepare() error {

	var err error

	pass_rd_struct := CovidAppLib.Covid_app{
		Location: rd_obj.CommonObj.Key,
	}

	rd_obj.CommonObj.Covid_data = &pass_rd_struct
	if rd_obj.CommonObj.Covid_data == nil {
		err = errors.New("Prepare() method failed for ReadOne.")
	}
	return err
}

func (rd_obj *RDONE) Exec() error {

	var rerr error
	var rd_strone = &CovidVaxData{}

	res_struct := &CovidAppLib.Covid_app{}
	//read operation
	err := rd_obj.CommonObj.Cli_obj.Read(rd_obj.CommonObj.Covid_data,
		rd_obj.CommonObj.Rncui, res_struct)

	if err != nil {
		fmt.Println("Read request failed !!", err)
		rd_strone.Status = -1
		rd_strone.Fill_ReadOne(rd_obj)
		rerr = errors.New("Exec() method failed for ReadOne")
	} else {
		fmt.Println("Result of the read request is: ", res_struct)
		total_vaccinations_int := strconv.Itoa(int(res_struct.Total_vaccinations))
		people_vaccinated_int := strconv.Itoa(int(res_struct.People_vaccinated))

		read_map := map[string]string{
			"Location":           res_struct.Location,
			"Iso_code":           res_struct.Iso_code,
			"Total_vaccinations": total_vaccinations_int,
			"People_vaccinated":  people_vaccinated_int,
		}
		//Fill write request data into a map.
		fill_data_into_map(read_map, rd_obj.CommonObj.Rncui)
		rd_strone.Status = 0
		rd_strone.Fill_ReadOne(rd_obj)
		rerr = nil
	}
	//Dump structure into json.
	rd_obj.CommonObj.Outfile_name = rd_strone.dump_into_json(rd_obj.CommonObj.Outfile_uuid)
	return rerr
}

func (rd_obj *RDONE) Complete() error {

	var c_err error
	//Copy temporary json file into json outfile.
	err := copy_tmp_file_to_json_file(rd_obj.CommonObj.Outfile_name,
		rd_obj.CommonObj.Json_filename)
	if err != nil {
		c_err = errors.New("Complete() method failed for ReadOne.")
	}
	return c_err
}

func (wm_obj *WRMUL) Prepare() error {

	var err error

	/*Create and Initialize the map for multiple
	keys and its rncui.*/
	key_rncui_map = make(map[string]string)

	//call function to parse csv file.
	fp := parse_csv_file(wm_obj.Csvfile)

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
		covid_wr_struct := &CovidAppLib.Covid_app{
			Location:           record[0],
			Iso_code:           record[1],
			Total_vaccinations: Total_vaccinations_int,
			People_vaccinated:  People_vaccinated_int,
		}

		//Fill the map for each structure of csv record.
		WriteMulti_map[*covid_wr_struct] = "record_struct"

		if WriteMulti_map == nil {
			err = errors.New("Prepare() method failed for WriteMulti operation.")
		} else {
			err = nil
		}
	}
	return err
}

func (wm_obj *WRMUL) Exec() error {

	var werr error
	var fill_wrdata = &CovidVaxData{}

	for csv_struct, val := range WriteMulti_map {
		fmt.Println(csv_struct, val)
		rncui := get_rncui_for_csvfile(key_rncui_map, &csv_struct)
		wm_obj.CommonObj.Key = csv_struct.Location
		wm_obj.CommonObj.Rncui = rncui
		err := wm_obj.CommonObj.Cli_obj.Write(&csv_struct, rncui)

		if err != nil {
			fmt.Println("Pmdb Write failed.", err)
			fill_wrdata.Status = -1
			werr = errors.New("Exec() method failed for WriteMulti operation.")
		} else {
			fmt.Println("Pmdb Write successful!")
			fill_wrdata.Status = 0
			werr = nil
		}
		fill_wrdata.Fill_WriteMulti(wm_obj)
	}
	//Dump structure into json.
	wm_obj.CommonObj.Outfile_name = fill_wrdata.dump_into_json(wm_obj.CommonObj.Outfile_uuid)
	return werr
}

func (wm_obj *WRMUL) Complete() error {
	var c_err error
	//Copy temporary json file into json outfile.
	err := copy_tmp_file_to_json_file(wm_obj.CommonObj.Outfile_name,
		wm_obj.CommonObj.Json_filename)
	if err != nil {
		c_err = errors.New("Complete() method failed for ReadOne.")
	}
	return c_err
}

func (rm_obj *RDMUL) Prepare() error {

	var err error
	var rm_rncui []string
	var rm_data []*CovidAppLib.Covid_app

	for rd_key, rd_rncui := range key_rncui_map {
		fmt.Println(rd_key, " ", rd_rncui)
		multi_rd_struct := CovidAppLib.Covid_app{
			Location: rd_key,
		}
		rm_rncui = append(rm_rncui, rd_rncui)
		rm_obj.Multi_rncui = rm_rncui
		rm_data = append(rm_data, &multi_rd_struct)
		rm_obj.Multi_rd = rm_data

		if rm_obj.Multi_rd == nil && rm_obj.Multi_rncui == nil {
			err = errors.New("Prepare() method failed for ReadMulti.")
		} else {
			err = nil
		}
	}
	return err
}

func (rm_obj *RDMUL) Exec() error {

	var rerr error
	//var reply_size int64
	var rd_strdata = &CovidVaxData{}

	if len(rm_obj.Multi_rd) == len(rm_obj.Multi_rncui) {
		for i := range rm_obj.Multi_rncui {
			res_struct := &CovidAppLib.Covid_app{}
			err := rm_obj.CommonObj.Cli_obj.Read(rm_obj.Multi_rd[i], rm_obj.Multi_rncui[i], res_struct)
			if err != nil {
				fmt.Println("Read request failed !!", err)
				rd_strdata.Status = -1
				rd_strdata.Fill_ReadMulti(rm_obj)
				rerr = errors.New("Exec() method failed for ReadMulti")
			} else {
				fmt.Println("Result of the read request is: ", res_struct)
				total_vaccinations_int := strconv.Itoa(int(res_struct.Total_vaccinations))
				people_vaccinated_int := strconv.Itoa(int(res_struct.People_vaccinated))

				read_map := map[string]string{
					"Location":           res_struct.Location,
					"Iso_code":           res_struct.Iso_code,
					"Total_vaccinations": total_vaccinations_int,
					"People_vaccinated":  people_vaccinated_int,
				}
				//Fill write request data into a map.
				fill_data_into_map(read_map, rm_obj.Multi_rncui[i])
				rd_strdata.Status = 0
				rd_strdata.Fill_ReadMulti(rm_obj)
				rerr = nil
			}
		}
	}
	//Dump structure into json.
	rm_obj.CommonObj.Outfile_name = rd_strdata.dump_into_json(rm_obj.CommonObj.Outfile_uuid)
	return rerr
}

func (rm_obj *RDMUL) Complete() error {
	var c_err error
	//Copy temporary json file into json outfile.
	err := copy_tmp_file_to_json_file(rm_obj.CommonObj.Outfile_name,
		rm_obj.CommonObj.Json_filename)
	if err != nil {
		c_err = errors.New("Complete() method failed for ReadMulti.")
	}
	return c_err
}

func (getleader *GetLeader) Prepare() error {

	var err error
	//Get timestamp.
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	leader_uuid := getleader.CommonObj.Cli_obj.GetLeader()
	app_leader := CovidVaxData{
		Raft_uuid:   raft_uuid_go,
		Client_uuid: peer_uuid_go,
		Leader_uuid: leader_uuid,
		Operation:   getleader.CommonObj.Input[0],
		Timestamp:   timestamp,
	}
	getleader.Leader_data = app_leader
	if getleader.Leader_data.Leader_uuid == "" {
		err = errors.New("Prepare() method failed for get_leader operation.")
	} else {
		err = nil
		fmt.Println("Leader uuid is:", getleader.Leader_data.Leader_uuid)
	}

	return err
}

func (getleader *GetLeader) Exec() error {

	var err error
	//Dump structure into json.
	temp_file := getleader.Leader_data.dump_into_json(getleader.CommonObj.Outfile_uuid)
	getleader.CommonObj.Outfile_name = temp_file
	if getleader.CommonObj.Outfile_name == "" {
		err = errors.New("Exec() method failed for get_leader operation")
	} else {
		err = nil
	}
	return err
}

func (getleader *GetLeader) Complete() error {

	err := copy_tmp_file_to_json_file(getleader.CommonObj.Outfile_name, getleader.CommonObj.Json_filename)
	return err
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

		fmt.Print("Enter operation(WriteOne/ WriteMulti/ ReadOne/ ReadMulti/ get_leader/ exit): ")

		//Create and Initialize map for write-read oufile.
		data = make(map[string]map[string]string)

		//Create and Initialize the map for WriteMulti
		WriteMulti_map = make(map[CovidAppLib.Covid_app]string)

		//Create temporary UUID
		temp_uuid := uuid.NewV4().String()
		//Get Leader
		leader_uuid := cli_obj.GetLeader()

		//Get console input string
		var str []string

		//Split the inout string.
		input := get_cmdline_input(str)

		operation := input[0]

		var rw_iface RDWR_iface

		switch operation {

		case "WriteOne":
			rw_iface = &WRONE{
				CommonObj: RDWR{
					Leader_uuid:   leader_uuid,
					Outfile_uuid:  temp_uuid,
					Json_filename: input[6],
					Key:           input[2],
					Rncui:         input[1],
					Input:         input,
					Cli_obj:       cli_obj,
				},
			}
		case "ReadOne":
			rw_iface = &RDONE{
				CommonObj: RDWR{
					Leader_uuid:   leader_uuid,
					Outfile_uuid:  temp_uuid,
					Json_filename: input[3],
					Key:           input[1],
					Rncui:         input[2],
					Input:         input,
					Cli_obj:       cli_obj,
				},
			}
		case "WriteMulti":
			rw_iface = &WRMUL{
				Csvfile: input[1],
				CommonObj: RDWR{
					Leader_uuid:   leader_uuid,
					Outfile_uuid:  temp_uuid,
					Json_filename: input[2],
					Input:         input,
					Cli_obj:       cli_obj,
				},
			}
		case "ReadMulti":
			rw_iface = &RDMUL{
				CommonObj: RDWR{
					Leader_uuid:   leader_uuid,
					Outfile_uuid:  temp_uuid,
					Json_filename: input[1],
					Input:         input,
					Cli_obj:       cli_obj,
				},
			}
		case "get_leader":
			rw_iface = &GetLeader{
				CommonObj: RDWR{
					Outfile_uuid:  temp_uuid,
					Json_filename: input[1],
					Input:         input,
					Cli_obj:       cli_obj,
				},
			}
		case "exit":
			os.Exit(0)
		default:
			fmt.Println("\nEnter valid operation: WriteOne/ReadOne/WriteMulti/ReadMulti/get_leader/exit")
		}
		//Call set of methods from interface
		P_err := rw_iface.Prepare()
		if P_err != nil {
			log.Fatal("error")
		}
		E_err := rw_iface.Exec()
		if E_err != nil {
			log.Fatal("error")
		}
		C_err := rw_iface.Complete()
		if C_err != nil {
			log.Fatal("error")
		}
	}
}
