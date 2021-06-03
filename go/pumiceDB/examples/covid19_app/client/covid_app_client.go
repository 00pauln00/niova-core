package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"encoding/csv"
	"encoding/json"
	"github.com/google/uuid"

	"covidapplib/lib"
	"niova/go-pumicedb-lib/client"
	"niova/go-pumicedb-lib/common"
)

/*
#include <stdlib.h>
*/
import "C"

var (
	raftUuid      string
	clientUuid    string
	jsonFilePath  string
	rwDataMap     map[string]map[string]string
	keyRncuiMap   map[string]string
	writeMultiMap map[CovidAppLib.Covid_locale]string
)

func main() {

	//Print help message.
	if len(os.Args) == 1 || os.Args[1] == "-help" || os.Args[1] == "-h" {
		fmt.Println("You need to pass the following arguments:")
		fmt.Println("Positional Arguments: \n		'-r' - RAFT UUID \n		'-u' - CLIENT UUID")
		fmt.Println("Optional Arguments: \n		'-l' - Json and Log File Path \n		-h, -help")
		fmt.Println("Pass arguments in this format: \n		./covid_app_client -r RAFT UUID -u CLIENT UUID")
		os.Exit(0)
	}

	//Parse the cmdline parameter
	parseFlag()

	//Create log directory if not Exist.
	makeDirectoryIfNotExists()

	//Create log file.
	initLogger()

	log.Info("Raft UUID: ", raftUuid)
	log.Info("Peer UUID: ", clientUuid)
	log.Info("Outfile Path: ", jsonFilePath)

	// sleep for 2sec.
	fmt.Println("Wait for 2 sec to start client")
	//Create new client object.
	cli_obj := PumiceDBClient.PmdbClientNew(raftUuid, clientUuid)
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
		input := getInput(str)
		ops := input[0]

		//Create and Initialize map for write-read oufile.
		rwDataMap = make(map[string]map[string]string)

		//Create and Initialize the map for WriteMulti
		writeMultiMap = make(map[CovidAppLib.Covid_locale]string)

		///Create temporary UUID
		temp_uuid := uuid.New()
		temp_uuid_str := temp_uuid.String()

		var op_iface Operation

		switch ops {
		case "WriteOne":
			op_iface = &wrOne{
				Op: opInfo{
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
					Outfile_uuid:  temp_uuid_str,
					Json_filename: input[2],
					Input:         input,
					Cli_obj:       cli_obj,
				},
			}
		case "ReadMulti":
			op_iface = &rdMul{
				Op: opInfo{
					Outfile_uuid:  temp_uuid_str,
					Json_filename: input[1],
					Input:         input,
					Cli_obj:       cli_obj,
				},
			}
		case "get_leader":
			op_iface = &getLeader{
				Op: opInfo{
					Json_filename: input[1],
					Input:         input,
					Cli_obj:       cli_obj,
				},
			}
		case "exit":
			os.Exit(0)
		default:
			fmt.Println("\nEnter valid operation: WriteOne/ReadOne/WriteMulti/ReadMulti/get_leader/exit")
			continue
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

//Positional Arguments.
func parseFlag() {

	flag.StringVar(&raftUuid, "r", "NULL", "raft uuid")
	flag.StringVar(&clientUuid, "u", "NULL", "peer uuid")
	flag.StringVar(&jsonFilePath, "l", "/tmp/covidAppLog", "json outfile path")

	flag.Parse()
}

//If log directory is not exist it creates directory.
//and if dir path is not passed then it will create log file
//in "/tmp/covidAppLog" path.
func makeDirectoryIfNotExists() error {
	if _, err := os.Stat(jsonFilePath); os.IsNotExist(err) {
		return os.Mkdir(jsonFilePath, os.ModeDir|0755)
	}
	return nil
}

func initLogger() {

	var filename string = jsonFilePath + "/" + clientUuid + ".log"
	fmt.Println("logfile:", filename)
	// Create the log file if doesn't exist. And append to it if it already exists.
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	Formatter := new(log.TextFormatter)
	// You can change the Timestamp format. But you have to use the same date and time.
	// "2006-02-02 15:04:06" Works. If you change any digit, it won't work
	// ie "Mon Jan 2 15:04:05 MST 2006" is the reference time. You can't change it
	Formatter.TimestampFormat = "02-01-2006 15:04:05"
	Formatter.FullTimestamp = true
	log.SetFormatter(Formatter)
	if err != nil {
		// Cannot open log file. Logging to stderr
		fmt.Println(err)
	} else {
		log.SetOutput(f)
	}

}

//Interface for operation.
type Operation interface {
	Prepare() error  //Fill Structure.
	Exec() error     //Write-Read Operation.
	Complete() error //Create Output Json File.
}

/*
 Structure for Common items.
*/
type opInfo struct {
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
	Op       opInfo
	PmdbData *PumiceDBCommon.PMDBInfo
}

/*
 Structure to create json outfile
*/
type covidVaxData struct {
	Operation string
	Status    int
	Timestamp string
	Data      map[string]map[string]string
}

//Function to write write_rwDataMap into map.
func fillDataToMap(mp map[string]string, rncui string) {

	//Fill rwDataMap into outer map.
	rwDataMap[rncui] = mp
}

//Method to dump CovidVaxData structure into json file.
func (cvd *covidVaxData) dumpIntoJson(outfile_uuid string) string {

	//Prepare path for temporary json file.
	temp_outfile_name := jsonFilePath + "/" + outfile_uuid + ".json"
	file, _ := json.MarshalIndent(cvd, "", "\t")
	_ = ioutil.WriteFile(temp_outfile_name, file, 0644)

	return temp_outfile_name

}

//read cmdline input.
func getInput(input []string) []string {

	//Read the key from console
	key := bufio.NewReader(os.Stdin)

	key_text, _ := key.ReadString('\n')

	// convert CRLF to LF
	key_text = strings.Replace(key_text, "\n", "", -1)
	input = strings.Split(key_text, "#")

	return input
}

/*This function stores rncui for all csv file
  rwDataMap into a keyRncuiMap and returns that rncui.
*/
func getRncui(keyRncuiMap map[string]string,
	cwr *CovidAppLib.Covid_locale) string {

	app_uuid := uuid.New()
	//Generate app_uuid.
	app_uuid_str := app_uuid.String()
	//Create rncui string.
	rncui := app_uuid_str + ":0:0:0:0"
	keyRncuiMap[cwr.Location] = rncui

	return rncui
}

//parse csv file.
func parseCSV(filename string) (fp *csv.Reader) {

	// Open the file
	csvfile, err := os.Open(filename)
	if err != nil {
		log.Fatal("Error to open the csv file", err)
	}

	// Skip first row (line)
	row1, err := bufio.NewReader(csvfile).ReadSlice('\n')
	if err != nil {
		log.Fatal("error")
	}
	_, err = csvfile.Seek(int64(len(row1)), io.SeekStart)
	if err != nil {
		log.Fatal("error")
	}
	// Parse the file
	fp = csv.NewReader(csvfile)

	return fp
}

//Fill the Json rwDataMap into map for WriteOne operation.
func (cvd *covidVaxData) FillWriteOne(wr_one *wrOne) {

	//Get timestamp.
	timestamp := time.Now().Format("2006-01-02 15:04:05")

	//fill the value into json structure.
	cvd.Operation = wr_one.Op.Input[0]
	cvd.Timestamp = timestamp
	cvd.Data = rwDataMap

	status_str := strconv.Itoa(int(cvd.Status))
	write_mp := map[string]string{
		"Key":    wr_one.Op.Key,
		"Status": status_str,
	}

	//fill write request rwDataMap into a map.
	fillDataToMap(write_mp, wr_one.Op.Rncui)

}

//Fill the Json rwDataMap into map for ReadOne operation.
func (cvd *covidVaxData) FillReadOne(rd_one *rdOne) {

	//Get timestamp.
	timestamp := time.Now().Format("2006-01-02 15:04:05")

	//Fill the value into Json structure.
	cvd.Operation = rd_one.Op.Input[0]
	cvd.Timestamp = timestamp
	cvd.Data = rwDataMap
}

//Fill the Json rwDataMap into map for WriteMulti operation.
func (cvd *covidVaxData) FillWriteMulti(wm *wrMul) {

	//get timestamp.
	timestamp := time.Now().Format("2006-01-02 15:04:05")

	//fill the value into json structure.
	cvd.Operation = wm.Op.Input[0]
	cvd.Timestamp = timestamp
	cvd.Data = rwDataMap

	status_str := strconv.Itoa(int(cvd.Status))
	write_mp := map[string]string{
		"Key":    wm.Op.Key,
		"Status": status_str,
	}

	//fill write request rwDataMap into a map.
	fillDataToMap(write_mp, wm.Op.Rncui)
}

//Fill the Json rwDataMap into map for ReadMulti operation.
func (cvd *covidVaxData) FillReadMulti(rm *rdMul) {

	//Get timestamp.
	timestamp := time.Now().Format("2006-01-02 15:04:05")

	//Fill the value into Json structure.
	cvd.Operation = rm.Op.Input[0]
	cvd.Timestamp = timestamp
	cvd.Data = rwDataMap
}

//Copy temporary outfile into actual Json file.
func copyToJsonFile(temp_outfile_name string, json_filename string) error {

	var cp_err error
	//Prepare json output filepath.
	json_outf := jsonFilePath + "/" + json_filename + ".json"
	//Create output json file.
	os.Create(json_outf)

	//Copy temporary json file into output json file.
	_, err := exec.Command("cp", temp_outfile_name, json_outf).Output()
	if err != nil {
		log.Error("%s", err)
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

	//typecast the rwDataMap type to int
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

/*Exec() method for  WriteOne to write rwDataMap
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
		log.Info("Write key-value failed : ", err)
	} else {
		log.Info("Pmdb Write successful!")
		fill_wrone.Status = 0
		err_msg = nil
	}
	fill_wrone.FillWriteOne(wr_obj)
	//Dump structure into json.
	wr_obj.Op.Outfile_name = fill_wrone.dumpIntoJson(wr_obj.Op.Outfile_uuid)

	return err_msg
}

/*Complete() method for WriteOne to
  create output Json file.*/
func (wr_obj *wrOne) Complete() error {

	var c_err error

	//Copy temporary json file into json outfile.
	err := copyToJsonFile(wr_obj.Op.Outfile_name,
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

/*Exec() method for  ReadOne to read rwDataMap
  and dump to json file.*/
func (rd_obj *rdOne) Exec() error {

	var rerr error
	var rd_strone = &covidVaxData{}

	res_struct := &CovidAppLib.Covid_locale{}
	//read operation
	err := rd_obj.Op.Cli_obj.Read(rd_obj.Op.Covid_data,
		rd_obj.Op.Rncui, res_struct)

	if err != nil {
		log.Info("Read request failed !!", err)
		rd_strone.Status = -1
		rd_strone.FillReadOne(rd_obj)
		rerr = errors.New("Exec() method failed for ReadOne")
	} else {
		log.Info("Result of the read request is: ", res_struct)
		total_vaccinations_int := strconv.Itoa(int(res_struct.Total_vaccinations))
		people_vaccinated_int := strconv.Itoa(int(res_struct.People_vaccinated))

		read_map := map[string]string{
			"Location":           res_struct.Location,
			"Iso_code":           res_struct.Iso_code,
			"Total_vaccinations": total_vaccinations_int,
			"People_vaccinated":  people_vaccinated_int,
		}
		//Fill write request rwDataMap into a map.
		fillDataToMap(read_map, rd_obj.Op.Rncui)
		rd_strone.Status = 0
		rd_strone.FillReadOne(rd_obj)
		rerr = nil
	}
	//Dump structure into json.
	rd_obj.Op.Outfile_name = rd_strone.dumpIntoJson(rd_obj.Op.Outfile_uuid)
	return rerr
}

/*Complete() method for ReadOne to
  create output Json file.*/
func (rd_obj *rdOne) Complete() error {

	var c_err error
	//Copy temporary json file into json outfile.
	err := copyToJsonFile(rd_obj.Op.Outfile_name,
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
	keyRncuiMap = make(map[string]string)

	//call function to parse csv file.
	fp := parseCSV(wm_obj.Csvfile)

	for {
		// Read each record from csv
		record, err := fp.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		//typecast the rwDataMap type to int
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
		writeMultiMap[*cwr] = "record_struct"

		if writeMultiMap == nil {
			err = errors.New("Prepare() method failed for WriteMulti operation.")
		} else {
			err = nil
		}
	}
	return err
}

/*Exec() method for WriteMulti to write rwDataMap
  from csv file and dump to json file.*/
func (wm_obj *wrMul) Exec() error {

	var werr error
	var wmData = &covidVaxData{}

	for csv_struct, _ := range writeMultiMap {
		rncui := getRncui(keyRncuiMap, &csv_struct)
		wm_obj.Op.Key = csv_struct.Location
		wm_obj.Op.Rncui = rncui
		err := wm_obj.Op.Cli_obj.Write(&csv_struct, rncui)

		if err != nil {
			wmData.Status = -1
			log.Info("Write key-value failed : ", err)
			werr = errors.New("Exec() method failed for WriteMulti operation.")
		} else {
			log.Info("Pmdb Write successful!")
			wmData.Status = 0
			werr = nil
		}
		wmData.FillWriteMulti(wm_obj)
	}
	//Dump structure into json.
	wm_obj.Op.Outfile_name = wmData.dumpIntoJson(wm_obj.Op.Outfile_uuid)
	return werr
}

/*Complete() method for WriteMulti to
  create output Json file.*/
func (wm_obj *wrMul) Complete() error {
	var c_err error
	//Copy temporary json file into json outfile.
	err := copyToJsonFile(wm_obj.Op.Outfile_name,
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
	var rm_rwDataMap []*CovidAppLib.Covid_locale

	for rd_key, rd_rncui := range keyRncuiMap {
		fmt.Println(rd_key, " ", rd_rncui)
		multi_rd_struct := CovidAppLib.Covid_locale{
			Location: rd_key,
		}
		rm_rncui = append(rm_rncui, rd_rncui)
		rm_obj.Multi_rncui = rm_rncui
		rm_rwDataMap = append(rm_rwDataMap, &multi_rd_struct)
		rm_obj.Multi_rd = rm_rwDataMap

		if rm_obj.Multi_rd == nil && rm_obj.Multi_rncui == nil {
			err = errors.New("Prepare() method failed for ReadMulti.")
		} else {
			err = nil
		}
	}
	return err
}

/*Exec() method for ReadMulti to read rwDataMap
  of csv file and dump to json file.*/
func (rm_obj *rdMul) Exec() error {

	var rerr error
	//var reply_size int64
	var rmData = &covidVaxData{}

	if len(rm_obj.Multi_rd) == len(rm_obj.Multi_rncui) {
		for i := range rm_obj.Multi_rncui {
			res_struct := &CovidAppLib.Covid_locale{}
			err := rm_obj.Op.Cli_obj.Read(rm_obj.Multi_rd[i], rm_obj.Multi_rncui[i], res_struct)
			if err != nil {
				log.Info("Read request failed !!", err)
				rmData.Status = -1
				rmData.FillReadMulti(rm_obj)
				rerr = errors.New("Exec() method failed for ReadMulti")
			} else {
				log.Info("Result of the read request is: ", res_struct)
				total_vaccinations_int := strconv.Itoa(int(res_struct.Total_vaccinations))
				people_vaccinated_int := strconv.Itoa(int(res_struct.People_vaccinated))

				read_map := map[string]string{
					"Location":           res_struct.Location,
					"Iso_code":           res_struct.Iso_code,
					"Total_vaccinations": total_vaccinations_int,
					"People_vaccinated":  people_vaccinated_int,
				}
				//Fill write request rwDataMap into a map.
				fillDataToMap(read_map, rm_obj.Multi_rncui[i])
				rmData.Status = 0
				rmData.FillReadMulti(rm_obj)
				rerr = nil
			}
		}
	}
	//Dump structure into json.
	rm_obj.Op.Outfile_name = rmData.dumpIntoJson(rm_obj.Op.Outfile_uuid)
	return rerr
}

/*Complete() method for ReadMulti to
  create output Json file.*/
func (rm_obj *rdMul) Complete() error {
	var c_err error
	//Copy temporary json file into json outfile.
	err := copyToJsonFile(rm_obj.Op.Outfile_name,
		rm_obj.Op.Json_filename)
	if err != nil {
		c_err = errors.New("Complete() method failed for ReadMulti.")
	}
	return c_err
}

//Prepare() method to get leader.
func (getleader *getLeader) Prepare() error {

	var err error

	Pmdb_items := &PumiceDBCommon.PMDBInfo{
		RaftUUID:   raftUuid,
		ClientUUID: clientUuid,
	}
	getleader.PmdbData = Pmdb_items
	if getleader.PmdbData == nil {
		err = errors.New("Prepare() method failed for get_leader operation.")
	} else {
		err = nil
	}
	return err
}

//Exec() method to get leader.
func (getleader *getLeader) Exec() error {

	var err error

	var leader_uuid uuid.UUID
	leader_uuid, err = getleader.Op.Cli_obj.PmdbGetLeader()
	if err != nil {
		fmt.Errorf("Failed to get Leader UUID")
	}
	leader_uuid_str := leader_uuid.String()
	getleader.PmdbData.LeaderUUID = leader_uuid_str
	if getleader.PmdbData.LeaderUUID == "" {
		err = errors.New("Exec() method failed for get_leader operation")
	} else {
		err = nil
		log.Info("Leader uuid is ", getleader.PmdbData.LeaderUUID)
	}
	return err
}

/*Complete() method for Get Leader to
  create output Json file.*/
func (getleader *getLeader) Complete() error {

	var cerr error
	//Prepare path for temporary json file.
	json_outfile := jsonFilePath + "/" + getleader.Op.Json_filename + ".json"
	file, err := json.MarshalIndent(getleader.PmdbData, "", "\t")
	err = ioutil.WriteFile(json_outfile, file, 0644)

	if err != nil {
		cerr = errors.New("Complete method for get leader operation failed")
	} else {
		cerr = nil
	}

	return cerr
}
