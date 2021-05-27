package main

import (
	"bufio"
	"covidapplib/lib"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/google/uuid"
	"io"
	"io/ioutil"
	"log"
	"niova/go-pumicedb-lib/client"
	"niova/go-pumicedb-lib/common"
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
	raft_uuid         string
	peer_uuid         string
	json_outfile_path string
	data              map[string]map[string]string
	key_rncui_map     map[string]string
	WriteMulti_map    map[CovidAppLib.Covid_locale]string
)

//Interface for operation.
type operation_iface interface {
	Prepare() error  //Fill Structure.
	Exec() error     //Write-Read Operation.
	Complete() error //Create Output Json File.
}

/*
 Structure for Common items.
*/
type opInfo struct {
	Pmdb_items    *PumiceDBCommon.PMDBInfo
	Outfile_uuid  string
	Outfile_name  string
	Json_filename string
	Key           string
	Rncui         string
	Input         []string
	Covid_data    *CovidAppLib.Covid_locale
	Cli_obj       *PumiceDBClient.PmdbClientObj
}

/*
 Structure for WriteOne operation
*/
type wrOne struct {
	Op opInfo
}

/*
 Structure for ReadOne operation
*/
type rdOne struct {
	Op opInfo
}

/*
 Structure for WriteMulti operation
*/
type wrMul struct {
	Csvfile string
	Op      opInfo
}

/*
 Structure for ReadMulti operation
*/
type rdMul struct {
	Multi_rd    []*CovidAppLib.Covid_locale
	Multi_rncui []string
	Op          opInfo
}

/*
 Structure for GetLeader operation
*/
type getLeader struct {
	Op         opInfo
	cvd        covidVaxData
	Pmdb_items *PumiceDBCommon.PMDBInfo
}

/*
 Structure to create json outfile
*/
type covidVaxData struct {
	Pmdb_items *PumiceDBCommon.PMDBInfo
	Operation  string
	Status     int
	Timestamp  string
	Data       map[string]map[string]string
}

//Function to write write_data into map.
func fill_data_into_map(mp map[string]string, rncui string) {

	//Fill data into outer map.
	data[rncui] = mp
}

//Method to dump CovidVaxData structure into json file.
func (cvd *covidVaxData) dump_into_json(outfile_uuid string) string {

	//Prepare path for temporary json file.
	temp_outfile_name := json_outfile_path + "/" + outfile_uuid + ".json"
	file, _ := json.MarshalIndent(cvd, "", "\t")
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
	cwr *CovidAppLib.Covid_locale) string {

	app_uuid := uuid.New()
	//Generate app_uuid.
	app_uuid_str := app_uuid.String()
	//Create rncui string.
	rncui := app_uuid_str + ":0:0:0:0"
	key_rncui_map[cwr.Location] = rncui

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

//Fill the Json data into map for WriteOne operation.
func (cvd *covidVaxData) Fill_WriteOne(wr_one *wrOne) {

	//Get timestamp.
	timestamp := time.Now().Format("2006-01-02 15:04:05")

	//fill the value into json structure.
	cvd.Pmdb_items = wr_one.Op.Pmdb_items
	cvd.Operation = wr_one.Op.Input[0]
	cvd.Timestamp = timestamp
	cvd.Data = data

	status_str := strconv.Itoa(int(cvd.Status))
	write_mp := map[string]string{
		"Key":    wr_one.Op.Key,
		"Status": status_str,
	}

	//fill write request data into a map.
	fill_data_into_map(write_mp, wr_one.Op.Rncui)

}

//Fill the Json data into map for ReadOne operation.
func (cvd *covidVaxData) Fill_ReadOne(rd_one *rdOne) {

	//Get timestamp.
	timestamp := time.Now().Format("2006-01-02 15:04:05")

	//Fill the value into Json structure.
	cvd.Pmdb_items = rd_one.Op.Pmdb_items
	cvd.Operation = rd_one.Op.Input[0]
	cvd.Timestamp = timestamp
	cvd.Data = data
}

//Fill the Json data into map for WriteMulti operation.
func (cvd *covidVaxData) Fill_WriteMulti(wm *wrMul) {

	//get timestamp.
	timestamp := time.Now().Format("2006-01-02 15:04:05")

	//fill the value into json structure.
	cvd.Pmdb_items = wm.Op.Pmdb_items
	cvd.Operation = wm.Op.Input[0]
	cvd.Timestamp = timestamp
	cvd.Data = data

	status_str := strconv.Itoa(int(cvd.Status))
	write_mp := map[string]string{
		"Key":    wm.Op.Key,
		"Status": status_str,
	}

	//fill write request data into a map.
	fill_data_into_map(write_mp, wm.Op.Rncui)
}

//Fill the Json data into map for ReadMulti operation.
func (cvd *covidVaxData) Fill_ReadMulti(rm *rdMul) {

	//Get timestamp.
	timestamp := time.Now().Format("2006-01-02 15:04:05")

	//Fill the value into Json structure.
	cvd.Pmdb_items = rm.Op.Pmdb_items
	cvd.Operation = rm.Op.Input[0]
	cvd.Timestamp = timestamp
	cvd.Data = data
}

//Copy temporary outfile into actual Json file.
func copy_tmp_file_to_json_file(temp_outfile_name string, json_filename string) error {

	var cp_err error
	//Prepare json output filepath.
	json_outf := json_outfile_path + "/" + json_filename + ".json"
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

//Prepare() method to fill structure for WriteOne.
func (wr_obj *wrOne) Prepare() error {

	var err error

	field0_val := wr_obj.Op.Input[3]
	field1_val := wr_obj.Op.Input[4]
	field2_val := wr_obj.Op.Input[5]

	//typecast the data type to int
	field1_int, _ := strconv.ParseInt(field1_val, 10, 64)
	field2_int, _ := strconv.ParseInt(field2_val, 10, 64)

	/*
	   Prepare the structure from values passed by user.
	   fill the struture
	*/
	pass_input_struct := CovidAppLib.Covid_locale{
		Location:           wr_obj.Op.Key,
		Iso_code:           field0_val,
		Total_vaccinations: field1_int,
		People_vaccinated:  field2_int,
	}
	wr_obj.Op.Covid_data = &pass_input_struct
	if wr_obj.Op.Covid_data == nil {
		err = errors.New("Prepare() method failed for WriteOne.")
	}
	return err
}

/*Exec() method for  WriteOne to write data
  and dump to json file.*/
func (wr_obj *wrOne) Exec() error {

	var err_msg error
	var fill_wrone = &covidVaxData{}

	//Perform write operation.
	err := wr_obj.Op.Cli_obj.Write(wr_obj.Op.Covid_data,
		wr_obj.Op.Rncui)

	if err != nil {
		err_msg = errors.New("Exec() method failed for WriteOne.")
		fill_wrone.Status = -1
		fmt.Println("Write key-value failed : ", err)
	} else {
		fmt.Println("Pmdb Write successful!")
		fill_wrone.Status = 0
		err_msg = nil
	}
	fill_wrone.Fill_WriteOne(wr_obj)
	//Dump structure into json.
	wr_obj.Op.Outfile_name = fill_wrone.dump_into_json(wr_obj.Op.Outfile_uuid)

	return err_msg
}

/*Complete() method for WriteOne to
  create output Json file.*/
func (wr_obj *wrOne) Complete() error {

	var c_err error

	//Copy temporary json file into json outfile.
	err := copy_tmp_file_to_json_file(wr_obj.Op.Outfile_name,
		wr_obj.Op.Json_filename)
	if err != nil {
		c_err = errors.New("Complete() method failed for WriteOne.")
	}
	return c_err
}

//Prepare() method to fill structure for ReadOne.
func (rd_obj *rdOne) Prepare() error {

	var err error

	pass_rd_struct := CovidAppLib.Covid_locale{
		Location: rd_obj.Op.Key,
	}

	rd_obj.Op.Covid_data = &pass_rd_struct
	if rd_obj.Op.Covid_data == nil {
		err = errors.New("Prepare() method failed for ReadOne.")
	}
	return err
}

/*Exec() method for  ReadOne to read data
  and dump to json file.*/
func (rd_obj *rdOne) Exec() error {

	var rerr error
	var rd_strone = &covidVaxData{}

	res_struct := &CovidAppLib.Covid_locale{}
	//read operation
	err := rd_obj.Op.Cli_obj.Read(rd_obj.Op.Covid_data,
		rd_obj.Op.Rncui, res_struct)

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
		fill_data_into_map(read_map, rd_obj.Op.Rncui)
		rd_strone.Status = 0
		rd_strone.Fill_ReadOne(rd_obj)
		rerr = nil
	}
	//Dump structure into json.
	rd_obj.Op.Outfile_name = rd_strone.dump_into_json(rd_obj.Op.Outfile_uuid)
	return rerr
}

/*Complete() method for ReadOne to
  create output Json file.*/
func (rd_obj *rdOne) Complete() error {

	var c_err error
	//Copy temporary json file into json outfile.
	err := copy_tmp_file_to_json_file(rd_obj.Op.Outfile_name,
		rd_obj.Op.Json_filename)
	if err != nil {
		c_err = errors.New("Complete() method failed for ReadOne.")
	}
	return c_err
}

//Prepare() method to fill structure for WriteMulti.
func (wm_obj *wrMul) Prepare() error {

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
		cwr := &CovidAppLib.Covid_locale{
			Location:           record[0],
			Iso_code:           record[1],
			Total_vaccinations: Total_vaccinations_int,
			People_vaccinated:  People_vaccinated_int,
		}

		//Fill the map for each structure of csv record.
		WriteMulti_map[*cwr] = "record_struct"

		if WriteMulti_map == nil {
			err = errors.New("Prepare() method failed for WriteMulti operation.")
		} else {
			err = nil
		}
	}
	return err
}

/*Exec() method for WriteMulti to write data
  from csv file and dump to json file.*/
func (wm_obj *wrMul) Exec() error {

	var werr error
	var fill_wrdata = &covidVaxData{}

	for csv_struct, _ := range WriteMulti_map {
		rncui := get_rncui_for_csvfile(key_rncui_map, &csv_struct)
		wm_obj.Op.Key = csv_struct.Location
		wm_obj.Op.Rncui = rncui
		err := wm_obj.Op.Cli_obj.Write(&csv_struct, rncui)

		if err != nil {
			fill_wrdata.Status = -1
			fmt.Println("Write key-value failed : ", err)
			werr = errors.New("Exec() method failed for WriteMulti operation.")
		} else {
			fmt.Println("Pmdb Write successful!")
			fill_wrdata.Status = 0
			werr = nil
		}
		fill_wrdata.Fill_WriteMulti(wm_obj)
	}
	//Dump structure into json.
	wm_obj.Op.Outfile_name = fill_wrdata.dump_into_json(wm_obj.Op.Outfile_uuid)
	fmt.Println("Temp filename:", wm_obj.Op.Outfile_name)
	return werr
}

/*Complete() method for WriteMulti to
  create output Json file.*/
func (wm_obj *wrMul) Complete() error {
	var c_err error
	//Copy temporary json file into json outfile.
	err := copy_tmp_file_to_json_file(wm_obj.Op.Outfile_name,
		wm_obj.Op.Json_filename)
	if err != nil {
		c_err = errors.New("Complete() method failed for ReadOne.")
	}
	return c_err
}

//Prepare() method to fill structure for ReadMulti.
func (rm_obj *rdMul) Prepare() error {

	var err error
	var rm_rncui []string
	var rm_data []*CovidAppLib.Covid_locale

	for rd_key, rd_rncui := range key_rncui_map {
		fmt.Println(rd_key, " ", rd_rncui)
		multi_rd_struct := CovidAppLib.Covid_locale{
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

/*Exec() method for ReadMulti to read data
  of csv file and dump to json file.*/
func (rm_obj *rdMul) Exec() error {

	var rerr error
	//var reply_size int64
	var rd_strdata = &covidVaxData{}

	if len(rm_obj.Multi_rd) == len(rm_obj.Multi_rncui) {
		for i := range rm_obj.Multi_rncui {
			res_struct := &CovidAppLib.Covid_locale{}
			err := rm_obj.Op.Cli_obj.Read(rm_obj.Multi_rd[i], rm_obj.Multi_rncui[i], res_struct)
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
	rm_obj.Op.Outfile_name = rd_strdata.dump_into_json(rm_obj.Op.Outfile_uuid)
	return rerr
}

/*Complete() method for ReadMulti to
  create output Json file.*/
func (rm_obj *rdMul) Complete() error {
	var c_err error
	//Copy temporary json file into json outfile.
	err := copy_tmp_file_to_json_file(rm_obj.Op.Outfile_name,
		rm_obj.Op.Json_filename)
	if err != nil {
		c_err = errors.New("Complete() method failed for ReadMulti.")
	}
	return c_err
}

//Prepare() method to get leader.
func (getleader *getLeader) Prepare() error {

	var err error
	//Get timestamp.
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	var leader_uuid uuid.UUID
	leader_uuid, err = getleader.Op.Cli_obj.PmdbGetLeader()
	if err != nil {
		fmt.Errorf("Failed to get Leader UUID")
	}
	leader_uuid_str := leader_uuid.String()
	fmt.Println("Leader uuid is ", leader_uuid_str)

	app_leader := covidVaxData{
		Pmdb_items: &PumiceDBCommon.PMDBInfo{
			Raft_uuid:   raft_uuid,
			Client_uuid: peer_uuid,
			Leader_uuid: leader_uuid_str,
		},
		Operation: getleader.Op.Input[0],
		Timestamp: timestamp,
	}
	getleader.cvd = app_leader
	if getleader.cvd.Pmdb_items.Leader_uuid == "" {
		err = errors.New("Prepare() method failed for get_leader operation.")
	} else {
		err = nil
		fmt.Println("Leader uuid is:", getleader.cvd.Pmdb_items.Leader_uuid)
	}

	return err
}

//Exec() method to get leader.
func (getleader *getLeader) Exec() error {

	var err error
	//Dump structure into json.
	temp_file := getleader.cvd.dump_into_json(getleader.Op.Outfile_uuid)
	getleader.Op.Outfile_name = temp_file
	if getleader.Op.Outfile_name == "" {
		err = errors.New("Exec() method failed for get_leader operation")
	} else {
		err = nil
	}
	return err
}

/*Complete() method for Get Leader to
  create output Json file.*/
func (getleader *getLeader) Complete() error {

	err := copy_tmp_file_to_json_file(getleader.Op.Outfile_name, getleader.Op.Json_filename)
	return err
}

//Positional Arguments.
func covid_app_getopts() {

	flag.StringVar(&raft_uuid, "r", "NULL", "raft uuid")
	flag.StringVar(&peer_uuid, "u", "NULL", "peer uuid")
	flag.StringVar(&json_outfile_path, "l", "NULL", "json outfile path")

	flag.Parse()

	fmt.Println("Raft UUID: ", raft_uuid)
	fmt.Println("Peer UUID: ", peer_uuid)
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
	covid_app_getopts()

	// sleep for 2sec.
	fmt.Println("Wait for 2 sec to start client")
	//Create new client object.
	cli_obj := PumiceDBClient.PmdbClientNew(raft_uuid, peer_uuid)
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

		//Get console input string
		var str []string

		//Split the inout string.
		input := get_cmdline_input(str)
		operation := input[0]

		//Create and Initialize map for write-read oufile.
		data = make(map[string]map[string]string)

		//Create and Initialize the map for WriteMulti
		WriteMulti_map = make(map[CovidAppLib.Covid_locale]string)

		///Create temporary UUID
		temp_uuid := uuid.New()
		temp_uuid_str := temp_uuid.String()

		//Get Leader
		lead_uuid, err := cli_obj.PmdbGetLeader()
		if err != nil {
			fmt.Errorf("Failed to get Leader UUID")
		}
		leader_uuid := lead_uuid.String()

		var op_iface operation_iface

		pmdb_item := &PumiceDBCommon.PMDBInfo{
			Raft_uuid:   raft_uuid,
			Client_uuid: peer_uuid,
			Leader_uuid: leader_uuid,
		}

		switch operation {
		case "WriteOne":
			op_iface = &wrOne{
				Op: opInfo{
					Pmdb_items:    pmdb_item,
					Outfile_uuid:  temp_uuid_str,
					Json_filename: input[6],
					Key:           input[2],
					Rncui:         input[1],
					Input:         input,
					Cli_obj:       cli_obj,
				},
			}
		case "ReadOne":
			op_iface = &rdOne{
				Op: opInfo{
					Pmdb_items:    pmdb_item,
					Outfile_uuid:  temp_uuid_str,
					Json_filename: input[3],
					Key:           input[1],
					Rncui:         input[2],
					Input:         input,
					Cli_obj:       cli_obj,
				},
			}
		case "WriteMulti":
			op_iface = &wrMul{
				Csvfile: input[1],
				Op: opInfo{
					Pmdb_items:    pmdb_item,
					Outfile_uuid:  temp_uuid_str,
					Json_filename: input[2],
					Input:         input,
					Cli_obj:       cli_obj,
				},
			}
		case "ReadMulti":
			op_iface = &rdMul{
				Op: opInfo{
					Pmdb_items:    pmdb_item,
					Outfile_uuid:  temp_uuid_str,
					Json_filename: input[1],
					Input:         input,
					Cli_obj:       cli_obj,
				},
			}
		case "get_leader":
			op_iface = &getLeader{
				Op: opInfo{
					Outfile_uuid:  temp_uuid_str,
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
		P_err := op_iface.Prepare()
		if P_err != nil {
			log.Fatal("error")
		}
		E_err := op_iface.Exec()
		if E_err != nil {
			log.Fatal("error")
		}
		C_err := op_iface.Complete()
		if C_err != nil {
			log.Fatal("error")
		}
	}
}
