package main

import (
	"bufio"
	"covidapplib/lib"
	//"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/satori/go.uuid"
	//"io"
	"io/ioutil"
	"log"
	"niova/go-pumicedb-lib/client"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
	"unsafe"
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
	wr_iface          WriteOne_iface
	rd_iface	  ReadOne_iface
)

//Interface for WriteOne
type WriteOne_iface interface {
	WriteOne_Prepare(string) *CovidAppLib.Covid_app
	WriteOne_Exec(string, string) int
	WriteOne_Complete(string, string)
}

//Interface for ReadOne
type ReadOne_iface interface {
	ReadOne_Prepare(string) *CovidAppLib.Covid_app
	ReadOne_Exec(string, string) unsafe.Pointer
	ReadOne_Complete(string, string)
}

/*
 Structure for WriteOne operation
*/
type WriteOneStruct struct {
	Leader_uuid   string
	Outfile_uuid  string
	Json_filename string
	Cli_obj       *PumiceDBClient.PmdbClientObj
}

/*
 Structure for ReadOne operation
*/
type ReadOneStruct struct {
	Leader_uuid   string
	Outfile_uuid  string
	Json_filename string
	Cli_obj       *PumiceDBClient.PmdbClientObj
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

//Method to dump zomato_app_output structure into json file.
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

func copy_tmp_file_to_json_file(temp_outfile_name string, json_filename string) {

	//Prepare json output filepath.
	json_outf := json_outfile_path + "/" + json_filename + ".json"

	fmt.Println("main outfile", json_outf)
	//Create output json file.
	os.Create(json_outf)

	//Copy temporary json file into output json file.
	_, err := exec.Command("cp", temp_outfile_name, json_outf).Output()
	if err != nil {
		fmt.Printf("%s", err)
	}

	//Remove temporary outfile after copying into json outfile.
	e := os.Remove(temp_outfile_name)
	if e != nil {
		log.Fatal(e)
	}

}

func (WO_obj *WriteOneStruct) WriteOne_Prepare(input []string) *CovidAppLib.Covid_app {

	//get_rncui := input[1]
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

	return &pass_input_struct
}

func (WO_obj *WriteOneStruct) WriteOne_Exec(input []string, rncui string) int {

	pass_input_struct := WO_obj.WriteOne_Prepare(input)

	//Get the actual size of the structure.
	len_wr_struct := WO_obj.Cli_obj.GetSize(pass_input_struct)
	fmt.Println("Length of the structure: ", len_wr_struct)

	//Perform write operation.
	rc := WO_obj.Cli_obj.Write(pass_input_struct, rncui)

	return rc
}

func (WO_obj *WriteOneStruct) WriteOne_Complete(input []string, rncui string) {

	pass_input_struct := WO_obj.WriteOne_Prepare(input)

	//Get timestamp.
	timestamp := time.Now().Format("2006-01-02 15:04:05")

	//Perform write operation.
	rc := WO_obj.WriteOne_Exec(input, rncui)

	if rc != 0 {
		fmt.Println("Pmdb Write failed.")
		write_strdata := CovidVaxData{
			Raft_uuid:   raft_uuid_go,
			Client_uuid: peer_uuid_go,
			Leader_uuid: WO_obj.Leader_uuid,
			Operation:   input[0],
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
		temp_outfile_name := write_strdata.dump_into_json(WO_obj.Outfile_uuid)

		//Copy temporary json file into json outfile.
		copy_tmp_file_to_json_file(temp_outfile_name, WO_obj.Json_filename)
	} else {
		fmt.Println("Pmdb Write successful!")
		write_strdata := CovidVaxData{
			Raft_uuid:   raft_uuid_go,
			Client_uuid: peer_uuid_go,
			Leader_uuid: WO_obj.Leader_uuid,
			Operation:   input[0],
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
		temp_outfile_name := write_strdata.dump_into_json(WO_obj.Outfile_uuid)

		//Copy temporary json file into json outfile.
		copy_tmp_file_to_json_file(temp_outfile_name, WO_obj.Json_filename)
	}

}

func (RO_obj *ReadOneStruct) ReadOne_Prepare(input []string) *CovidAppLib.Covid_app {

	key := input[1]

	pass_rd_struct := CovidAppLib.Covid_app{
		Location: key,
	}

	return &pass_rd_struct
}

func (RO_obj *ReadOneStruct) ReadOne_Exec(input []string, rd_rncui string) unsafe.Pointer {

	rd_rncui = input[1]

	pass_rd_struct := RO_obj.ReadOne_Prepare(input)

	length_rd_struct := RO_obj.Cli_obj.GetSize(pass_rd_struct)

	//Print all values which passed to Read()
	fmt.Println("Covid Structure for read:", pass_rd_struct)
	fmt.Println("Length of read:", length_rd_struct)

	var reply_size int64

	//read operation
	reply_buff := RO_obj.Cli_obj.Read(pass_rd_struct, rd_rncui,
		&reply_size)

	return reply_buff

}

func (RO_obj *ReadOneStruct) ReadOne_Complete(input []string, rd_rncui string) {

	timestamp := time.Now().Format("2006-01-02 15:04:05")
	reply_buff := RO_obj.ReadOne_Exec(input, rd_rncui)

	var reply_size int64

	if reply_buff == nil {

		rc := -1

		fmt.Println("Read request failed !!")

		rd_strdata := CovidVaxData{
			Raft_uuid:   raft_uuid_go,
			Client_uuid: peer_uuid_go,
			Leader_uuid: RO_obj.Leader_uuid,
			Operation:   input[0],
			Status:      rc,
			Timestamp:   timestamp,
			Data:        nil,
		}

		//Dump structure into json.
		temp_outfile_name := rd_strdata.dump_into_json(RO_obj.Outfile_uuid)

		//Copy temporary json file into json outfile.
		copy_tmp_file_to_json_file(temp_outfile_name, RO_obj.Json_filename)

	} else {
		rc := 0

		struct_op := &CovidAppLib.Covid_app{}
		RO_obj.Cli_obj.Decode(reply_buff, struct_op, reply_size)

		fmt.Println("Result of the read request is: ", struct_op)

		rd_strdata := CovidVaxData{
			Raft_uuid:   raft_uuid_go,
			Client_uuid: peer_uuid_go,
			Leader_uuid: RO_obj.Leader_uuid,
			Operation:   input[0],
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
		temp_outfile_name := rd_strdata.dump_into_json(RO_obj.Outfile_uuid)

		//Copy temporary json file into json outfile.
		copy_tmp_file_to_json_file(temp_outfile_name, RO_obj.Json_filename)

	}
	// Application should free the this reply_buff which is allocate by pmdb lib
	C.free(reply_buff)
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

		switch operation {

		case "WriteOne":
			get_rncui := input[1]
			outfile_name := input[6]
			wr_iface := WriteOneStruct{
				Leader_uuid:   leader_uuid,
				Outfile_uuid:  temp_uuid,
				Json_filename: outfile_name,
				Cli_obj:       cli_obj,
			}
			//call set of methods.
			wr_iface.WriteOne_Prepare(input)
			wr_iface.WriteOne_Exec(input, get_rncui)
			wr_iface.WriteOne_Complete(input, get_rncui)
		case "ReadOne":
			rd_rncui := input[2]
                        outfile_name := input[3]
			rd_iface := ReadOneStruct{
                                Leader_uuid:   leader_uuid,
                                Outfile_uuid:  temp_uuid,
                                Json_filename: outfile_name,
                                Cli_obj:       cli_obj,
                        }
			//call set of methods.
                        rd_iface.ReadOne_Prepare(input)
                        rd_iface.ReadOne_Exec(input, rd_rncui)
                        rd_iface.ReadOne_Complete(input, rd_rncui)

		case "exit":
			os.Exit(0)
		default:
			fmt.Println("\nEnter valid operation: WriteOne/ReadOne/WriteMulti/ReadMulti/get_leader/exit")
		}
	}
}
