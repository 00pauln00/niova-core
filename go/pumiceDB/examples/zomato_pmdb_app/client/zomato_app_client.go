package main

import (
	"bufio"
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
)

// Creating an interface for client.
type zomato_cli interface {
	// Methods
	Prepare() error
	Exec() error
	Complete() error
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

type wr_req_info struct {
	Key          string
	Rncui        string
	Operation    string
	Json_fname   string
	Leader_uuid  string
	Outfile_uuid string
	Cli_obj      *PumiceDBClient.PmdbClientObj
	Zomato_data  *zomatoapplib.Zomato_Data
	Outfilename  string
}

type WriteOne struct {
	Args        []string
	Wr_req_info *wr_req_info
}

type WriteMulti struct {
	Csv_fpath     string
	Wr_req_info   *wr_req_info
	Multi_reqdata []*zomatoapplib.Zomato_Data
}
type ReadOne struct {
	Wr_req_info *wr_req_info
}
type ReadMulti struct {
	Wr_req_info *wr_req_info
	Rm_rncui    []string
	Rmdata      []*zomatoapplib.Zomato_Data
}
type GetLeader struct {
	Wr_req_info *wr_req_info
	Leadt       *zomato_app_info
}

func (prep_wmstruct *WriteMulti) Get_struct_info() []*zomatoapplib.Zomato_Data {

	var multireq_dt []*zomatoapplib.Zomato_Data
	//Open the file.
	csvfile, err := os.Open(prep_wmstruct.Csv_fpath)
	if err != nil {
		log.Fatalln("Couldn't open the csv file", err)
	}
	//Parse the file, Skip first row (line)
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
		multireq_dt = append(multireq_dt, &struct_data)
	}
	return multireq_dt
}

func (prep_rstrct *ReadOne) Display_rdop_and_fill_struct(rd_data *zomatoapplib.Zomato_Data) {
	rest_id := strconv.Itoa(int(rd_data.Restaurant_id))
	rest_votes := strconv.Itoa(int(rd_data.Votes))
	fmt.Println("\nResult of the read request is: \nRestaurant id (key) = " + rest_id + "\nRestaurant name = " + rd_data.Restaurant_name + "\nCity = " + rd_data.City + "\nCuisines = " + rd_data.Cuisines + "\nRatings_text = " + rd_data.Ratings_text + "\nVotes = " + rest_votes)

	rd_req_mp := map[string]string{
		"Restaurant_id":   rest_id,
		"Restaurant_name": rd_data.Restaurant_name,
		"city":            rd_data.City,
		"cuisines":        rd_data.Cuisines,
		"ratings_text":    rd_data.Ratings_text,
		"votes":           rest_votes,
	}
	//Fill write request data into a map.
	fill_data_into_map(rd_req_mp, prep_rstrct.Wr_req_info.Rncui)
	strdata := &zomato_app_info{Status: 0}
	strdata.Fill_rstruct(prep_rstrct)
}
func (prep_rmstruct *ReadMulti) Display_rdmulop_and_fill_struct(strdata *zomato_app_info, rd_data *zomatoapplib.Zomato_Data, i int) {
	rest_id := strconv.Itoa(int(rd_data.Restaurant_id))
	rest_votes := strconv.Itoa(int(rd_data.Votes))
	fmt.Println("\nResult of the read request is: \nRestaurant id (key) = " + rest_id + "\nRestaurant name = " + rd_data.Restaurant_name + "\nCity = " + rd_data.City + "\nCuisines = " + rd_data.Cuisines + "\nRatings_text = " + rd_data.Ratings_text + "\nVotes = " + rest_votes)

	rd_req_mp := map[string]string{
		"Restaurant_id":   rest_id,
		"Restaurant_name": rd_data.Restaurant_name,
		"city":            rd_data.City,
		"cuisines":        rd_data.Cuisines,
		"ratings_text":    rd_data.Ratings_text,
		"votes":           rest_votes,
	}
	//Fill write request data into a map.
	fill_data_into_map(rd_req_mp, prep_rmstruct.Rm_rncui[i])
	strdata.Status = 0
	strdata.Fill_rmstruct(prep_rmstruct)
}

func (struct_zinfo *zomato_app_info) Fill_struct(wone *WriteOne) {
	//Get timestamp.
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	struct_zinfo.Raft_uuid = raft_uuid_go
	struct_zinfo.Client_uuid = client_uuid_go
	struct_zinfo.Leader_uuid = wone.Wr_req_info.Leader_uuid
	struct_zinfo.Operation = wone.Args[0]
	struct_zinfo.Timestamp = timestamp
	struct_zinfo.Data = data
	status := strconv.Itoa(int(struct_zinfo.Status))
	wr_mp := map[string]string{
		"Key":    wone.Args[2],
		"Status": status,
	}
	//Fill write request data into a map.
	fill_data_into_map(wr_mp, wone.Args[1])
	//Dump structure into json.
	temp_outfname := struct_zinfo.dump_into_json(wone.Wr_req_info.Outfile_uuid)
	wone.Wr_req_info.Outfilename = temp_outfname
}

func (struct_wminfo *zomato_app_info) Fill_wmstruct(wmul *WriteMulti) {
	//Get timestamp.
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	struct_wminfo.Raft_uuid = raft_uuid_go
	struct_wminfo.Client_uuid = client_uuid_go
	struct_wminfo.Leader_uuid = wmul.Wr_req_info.Leader_uuid
	struct_wminfo.Operation = wmul.Wr_req_info.Operation
	struct_wminfo.Timestamp = timestamp
	struct_wminfo.Data = data
	status := strconv.Itoa(int(struct_wminfo.Status))
	write_mp := map[string]string{
		"Key":    wmul.Wr_req_info.Key,
		"Status": status,
	}
	//Fill write request data into a map.
	fill_data_into_map(write_mp, wmul.Wr_req_info.Rncui)
}

func (struct_rinfo *zomato_app_info) Fill_rstruct(rone *ReadOne) {
	//Get timestamp.
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	struct_rinfo.Raft_uuid = raft_uuid_go
	struct_rinfo.Client_uuid = client_uuid_go
	struct_rinfo.Leader_uuid = rone.Wr_req_info.Leader_uuid
	struct_rinfo.Operation = rone.Wr_req_info.Operation
	struct_rinfo.Timestamp = timestamp
	struct_rinfo.Data = data
	//Dump structure into json.
	tmpout_filename := struct_rinfo.dump_into_json(rone.Wr_req_info.Outfile_uuid)
	rone.Wr_req_info.Outfilename = tmpout_filename
}
func (struct_rminfo *zomato_app_info) Fill_rmstruct(zrm *ReadMulti) {
	//Get timestamp.
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	struct_rminfo.Raft_uuid = raft_uuid_go
	struct_rminfo.Client_uuid = client_uuid_go
	struct_rminfo.Leader_uuid = zrm.Wr_req_info.Leader_uuid
	struct_rminfo.Operation = zrm.Wr_req_info.Operation
	struct_rminfo.Timestamp = timestamp
	struct_rminfo.Data = data
}
func (prep_strct *WriteOne) Prepare() error {
	var err error
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
	prep_strct.Wr_req_info.Zomato_data = &struct_data_cmdline
	//Open file for storing key, rncui in append mode.
	file, err := os.OpenFile("key_rncui_data.txt", os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}
	defer file.Close()
	//Append key_rncui in file.
	_, err_wr := file.WriteString("key, rncui = " + rest_id_string + "  " + rncui + "\n")
	if err_wr != nil {
		log.Fatal(err)
	}
	if prep_strct.Wr_req_info.Zomato_data == nil {
		err = errors.New("Prepare method for WriteOne failed")
	}
	return err
}

func (exec_strct *WriteOne) Exec() error {

	var error_msg error
	var write_strdata_cmd zomato_app_info
	//Perform write operation.
	err := exec_strct.Wr_req_info.Cli_obj.Write(exec_strct.Wr_req_info.Zomato_data, exec_strct.Args[1])
	if err != nil {
		fmt.Println("Write key-value failed : ", err)
		write_strdata_cmd.Status = -1
		error_msg = errors.New("Exec method for WriteOne Operation failed.")
	} else {
		fmt.Println("Pmdb Write successful!")
		write_strdata_cmd.Status = 0
		error_msg = nil
	}
	write_strdata_cmd.Fill_struct(exec_strct)
	return error_msg
}

func (com_strct *WriteOne) Complete() error {

	var cerr error
	//Copy contents in json outfile.
	err := copy_to_outfile(com_strct.Wr_req_info.Outfilename, com_strct.Args[8])
	if err != nil {
		cerr = errors.New("Complete method for WriteOne Operation failed")
	}
	return cerr
}

func (prep_rstrct *ReadOne) Prepare() error {

	var err error
	//Typecast key into int64.
	key_int64, _ := strconv.ParseInt(prep_rstrct.Wr_req_info.Key, 10, 64)
	//Fill the Zomato_App structure.
	struct_rd := zomatoapplib.Zomato_Data{
		Restaurant_id: key_int64,
	}
	prep_rstrct.Wr_req_info.Zomato_data = &struct_rd
	if prep_rstrct.Wr_req_info.Zomato_data == nil {
		err = errors.New("Prepare method for ReadOne Operation failed")
	} else {
		err = nil
	}
	return err

}

func (prep_rstrct *ReadOne) Exec() error {

	var roerr error
	//Perform read operation.
	op_struct := &zomatoapplib.Zomato_Data{}
	err := prep_rstrct.Wr_req_info.Cli_obj.Read(prep_rstrct.Wr_req_info.Zomato_data, prep_rstrct.Wr_req_info.Rncui, op_struct)
	if err != nil {
		fmt.Println("Read request failed !!", err)
		strdata := &zomato_app_info{Status: -1}
		strdata.Fill_rstruct(prep_rstrct)
		roerr = errors.New("Exec method for ReadOne Operation failed")
	}
	prep_rstrct.Display_rdop_and_fill_struct(op_struct)
	return roerr
}

func (prep_rstrct *ReadOne) Complete() error {

	var cerr error
	//Copy contents in json outfile.
	err := copy_to_outfile(prep_rstrct.Wr_req_info.Outfilename, prep_rstrct.Wr_req_info.Json_fname)
	if err != nil {
		cerr = errors.New("Complete method for ReadOne Operation failed")
	}
	return cerr
}

func (prep_wmstruct *WriteMulti) Prepare() error {

	var wmperr error
	//Get array of zomato_data structure.
	mreqdata := prep_wmstruct.Get_struct_info()
	prep_wmstruct.Multi_reqdata = mreqdata
	if prep_wmstruct.Multi_reqdata == nil {
		wmperr = errors.New("Prepare method for WriteMulti Operation failed")
	} else {
		wmperr = nil
	}
	return wmperr
}

func (prep_wmstruct *WriteMulti) Exec() error {

	var excerr error
	var wr_strdata = &zomato_app_info{}
	//Create a file for storing keys and rncui.
	os.Create("key_rncui_data.txt")
	//Open file for storing key, rncui in append mode.
	file, err := os.OpenFile("key_rncui_data.txt", os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}
	defer file.Close()
	for i := 0; i < len(prep_wmstruct.Multi_reqdata); i++ {
		//Generate app_uuid.
		app_uuid := uuid.NewV4().String()
		//Create rncui string.
		rncui := app_uuid + ":0:0:0:0"
		//Append key_rncui in file.
		rest_id_str := strconv.Itoa(int(prep_wmstruct.Multi_reqdata[i].Restaurant_id))
		_, err_write := file.WriteString("key, rncui = " + rest_id_str + "  " + rncui + "\n")
		if err_write != nil {
			log.Fatal(err)
		}
		prep_wmstruct.Wr_req_info.Key = rest_id_str
		prep_wmstruct.Wr_req_info.Rncui = rncui

		err := prep_wmstruct.Wr_req_info.Cli_obj.Write(prep_wmstruct.Multi_reqdata[i], rncui)
		if err != nil {
			fmt.Println("Pmdb Write failed.", err)
			wr_strdata.Status = -1
			excerr = errors.New("Exec method for WriteMulti Operation failed")
		} else {
			fmt.Println("Pmdb Write successful!")
			wr_strdata.Status = 0
			excerr = nil
		}
		wr_strdata.Fill_wmstruct(prep_wmstruct)
	}
	//Dump structure into json.
	temp_outfname := wr_strdata.dump_into_json(prep_wmstruct.Wr_req_info.Outfile_uuid)
	prep_wmstruct.Wr_req_info.Outfilename = temp_outfname
	return excerr
}

func (prep_wmstruct *WriteMulti) Complete() error {
	var cerr error
	//Copy contents in json outfile.
	err := copy_to_outfile(prep_wmstruct.Wr_req_info.Outfilename, prep_wmstruct.Wr_req_info.Json_fname)
	if err != nil {
		cerr = errors.New("Complete method for WriteMulti Operation failed")
	}
	return cerr
}

func (prep_rmstruct *ReadMulti) Prepare() error {
	var prerr error
	var rmreq_dt []*zomatoapplib.Zomato_Data
	var rmrncui []string
	f, err := os.Open("key_rncui_data.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		rdall_data := strings.Split(scanner.Text(), " ")
		rdall_key := rdall_data[3]
		rdall_rncui := rdall_data[5]
		//Typecast key into int64.
		key_int64, _ := strconv.ParseInt(rdall_key, 10, 64)
		//Fill the Zomato_App structure.
		struct_rd := zomatoapplib.Zomato_Data{
			Restaurant_id: key_int64,
		}
		rmrncui = append(rmrncui, rdall_rncui)
		rmreq_dt = append(rmreq_dt, &struct_rd)
		prep_rmstruct.Rm_rncui = rmrncui
		prep_rmstruct.Rmdata = rmreq_dt
	}
	if prep_rmstruct.Rmdata == nil && prep_rmstruct.Rm_rncui == nil {
		prerr = errors.New("Prepare method for ReadMulti Operation failed")
	} else {
		prerr = nil
	}
	return prerr
}
func (prep_rmstruct *ReadMulti) Exec() error {
	var rmexcerr error
	var strdata = &zomato_app_info{}
	if len(prep_rmstruct.Rmdata) == len(prep_rmstruct.Rm_rncui) {
		for i := range prep_rmstruct.Rmdata {
			//Perform read operation.
			rmop_struct := &zomatoapplib.Zomato_Data{}
			err := prep_rmstruct.Wr_req_info.Cli_obj.Read(prep_rmstruct.Rmdata[i], prep_rmstruct.Rm_rncui[i], rmop_struct)
			if err != nil {
				strdata = &zomato_app_info{Status: -1}
				strdata.Fill_rmstruct(prep_rmstruct)
				rmexcerr = errors.New("Exec method for ReadOne Operation failed")
			} else {
				prep_rmstruct.Display_rdmulop_and_fill_struct(strdata, rmop_struct, i)
			}
		}
	}
	//Dump structure into json.
	temp_outfname := strdata.dump_into_json(prep_rmstruct.Wr_req_info.Outfile_uuid)
	prep_rmstruct.Wr_req_info.Outfilename = temp_outfname
	return rmexcerr
}
func (prep_rmstruct *ReadMulti) Complete() error {
	var cerr error
	//Copy contents in json outfile.
	err := copy_to_outfile(prep_rmstruct.Wr_req_info.Outfilename, prep_rmstruct.Wr_req_info.Json_fname)
	if err != nil {
		cerr = errors.New("Complete method for ReadMulti Operation failed")
	}
	return cerr
}

func (get_leader *GetLeader) Prepare() error {

	var gleaerr error
	//Get timestamp.
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	lea_uuid := get_leader.Wr_req_info.Cli_obj.GetLeader()
	zai_leader := zomato_app_info{
		Raft_uuid:   raft_uuid_go,
		Client_uuid: client_uuid_go,
		Leader_uuid: lea_uuid,
		Operation:   get_leader.Wr_req_info.Operation,
		Timestamp:   timestamp,
	}
	get_leader.Leadt = &zai_leader
	if get_leader.Leadt.Leader_uuid == "" {
		gleaerr = errors.New("Prepare method for get leader operation failed")
	} else {
		gleaerr = nil
		fmt.Println("Leader uuid is:", get_leader.Leadt.Leader_uuid)
	}
	return gleaerr
}

func (get_leader *GetLeader) Exec() error {

	var glexcerr error
	//Dump structure into json.
	tempfname := get_leader.Leadt.dump_into_json(get_leader.Wr_req_info.Outfile_uuid)
	get_leader.Wr_req_info.Outfilename = tempfname
	if get_leader.Wr_req_info.Outfilename == "" {
		glexcerr = errors.New("Exec method for get leader operation failed")
	} else {
		glexcerr = nil
	}
	return glexcerr
}

func (get_leader *GetLeader) Complete() error {

	var cerr error
	err := copy_to_outfile(get_leader.Wr_req_info.Outfilename, get_leader.Wr_req_info.Json_fname)
	if err != nil {
		cerr = errors.New("Complete method for get leader operation failed")
	} else {
		cerr = nil
	}
	return cerr
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
func copy_to_outfile(tmp_outfile_name, jsonfilename string) error {
	var errcp error
	//Prepare json output filepath.
	json_outf := json_outfilepath + "/" + jsonfilename + ".json"
	//Create output json file.
	os.Create(json_outf)
	//Copy temporary json file into output json file.
	_, err := exec.Command("cp", tmp_outfile_name, json_outf).Output()
	if err != nil {
		fmt.Print("%s", err)
		errcp = err
	} else {
		errcp = nil
	}
	//Remove temporary outfile after copying into json outfile.
	os.Remove(tmp_outfile_name)
	return errcp
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

		//Declare interface variable.
		var zci zomato_cli
		switch ops {

		case "WriteOne":
			zci = &WriteOne{
				Args: ops_split,
				Wr_req_info: &wr_req_info{
					Leader_uuid:  leader_uuid,
					Outfile_uuid: outf_uuid,
					Cli_obj:      cli_obj,
				},
			}
		case "ReadOne":
			zci = &ReadOne{
				Wr_req_info: &wr_req_info{
					Key:          ops_split[1],
					Rncui:        ops_split[2],
					Json_fname:   ops_split[3],
					Operation:    ops,
					Leader_uuid:  leader_uuid,
					Outfile_uuid: outf_uuid,
					Cli_obj:      cli_obj,
				},
			}
		case "WriteMulti":
			zci = &WriteMulti{
				Csv_fpath: ops_split[1],
				Wr_req_info: &wr_req_info{
					Operation:    ops,
					Json_fname:   ops_split[2],
					Leader_uuid:  leader_uuid,
					Outfile_uuid: outf_uuid,
					Cli_obj:      cli_obj,
				},
			}
		case "ReadMulti":
			zci = &ReadMulti{
				Wr_req_info: &wr_req_info{
					Operation:    ops,
					Json_fname:   ops_split[1],
					Leader_uuid:  leader_uuid,
					Outfile_uuid: outf_uuid,
					Cli_obj:      cli_obj,
				},
			}
		case "get_leader":
			zci = &GetLeader{
				Wr_req_info: &wr_req_info{
					Operation:    ops,
					Json_fname:   ops_split[1],
					Outfile_uuid: outf_uuid,
					Cli_obj:      cli_obj,
				},
			}
		case "exit":
			os.Exit(0)
		case "default":
			fmt.Println("Enter valid operation: (WriteOne/ WriteMulti/ ReadOne/ ReadMulti/ get_leader /exit)")
		}

		//Perform Operations.
		prmerr := zci.Prepare()
		if prmerr != nil {
			log.Fatal(prmerr)
		}
		excerr := zci.Exec()
		if excerr != nil {
			log.Fatal(excerr)
		}
		cerr := zci.Complete()
		if cerr != nil {
			log.Fatal(cerr)
		}
	}

}
