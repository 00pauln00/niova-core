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
	"niova/go-pumicedb-lib/common"
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
	raftUuid     string
	clientUuid   string
	jsonOutFpath string
	data         map[string]map[string]string
)

// Creating an interface for client.
type ZomatoCli interface {
	// Methods
	Prepare() error
	Exec() error
	Complete() error
}

//Structure definition to dump into json.
type zomatoRqOp struct {
	Operation string
	Status    int
	Timestamp string
	Data      map[string]map[string]string
}

//Structure definition to store request specific information.
type rqInfo struct {
	Key          string
	Rncui        string
	Operation    string
	Status       int
	Json_fname   string
	Outfile_uuid string
	Cli_obj      *PumiceDBClient.PmdbClientObj
	Zomato_data  *zomatoapplib.Zomato_Data
	Outfilename  string
}

//Structure definition for writeOne operation.
type writeOne struct {
	Args []string
	Rq   *rqInfo
}

//Structure definition for writeMulti operation.
type writeMulti struct {
	Csv_fpath     string
	Rq            *rqInfo
	Multi_reqdata []*zomatoapplib.Zomato_Data
}

//Structure definition for readOne operation.
type readOne struct {
	Rq *rqInfo
}

//Structure definition for readMulti operation.
type readMulti struct {
	Rq       *rqInfo
	Rm_rncui []string
	Rmdata   []*zomatoapplib.Zomato_Data
}

//Structure definition for getLeader operation.
type getLeader struct {
	Rq       *rqInfo
	PmdbInfo *PumiceDBCommon.PMDBInfo
}

//Method to parse csv file and fill the required structures for writemulti operation.
func (wm *writeMulti) getWmInfo() []*zomatoapplib.Zomato_Data {

	var multireq_dt []*zomatoapplib.Zomato_Data
	//Open the file.
	csvfile, err := os.Open(wm.Csv_fpath)
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
		rest_id_str, err := strconv.ParseInt(record[0], 10, 64)
		if err != nil {
			fmt.Println("Error occured in typecasting Restaurant_id to int64")
		}
		//Typecast Votes to int64.
		votes_str, err := strconv.ParseInt(record[5], 10, 64)
		if err != nil {
			fmt.Println("Error occured in typecasting Votes to int64")
		}

		//Fill the Zomato_App structure.
		zm_dt := zomatoapplib.Zomato_Data{
			Restaurant_id:   rest_id_str,
			Restaurant_name: record[1],
			City:            record[2],
			Cuisines:        record[3],
			Ratings_text:    record[4],
			Votes:           votes_str,
		}
		multireq_dt = append(multireq_dt, &zm_dt)
	}
	return multireq_dt
}

//Method to display output of read one operation and fill the structure.
func (dro *readOne) displayAndFill(rd_data *zomatoapplib.Zomato_Data) {
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
	fillMap(rd_req_mp, dro.Rq.Rncui)
	strdata := &zomatoRqOp{Status: 0}
	strdata.fillRo(dro)
}

//Method to display output of read multi operation and fill the structure.
func (drm *readMulti) displayAndFill(rmdt *zomatoRqOp, rd_data *zomatoapplib.Zomato_Data, i int) {
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
	fillMap(rd_req_mp, drm.Rm_rncui[i])
	rmdt.Status = 0
	rmdt.fillRm(drm)
}

//Method to fill the output of write one operation.
func (wo *zomatoRqOp) fillWo(wone *writeOne) {

	//Get timestamp.
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	wo.Operation = wone.Args[0]
	wo.Timestamp = timestamp
	wo.Data = data
	status := strconv.Itoa(int(wo.Status))
	wr_mp := map[string]string{
		"Key":    wone.Args[2],
		"Status": status,
	}
	//Fill write request data into a map.
	fillMap(wr_mp, wone.Args[1])
	//Dump structure into json.
	temp_outfname := wo.dumpIntoJson(wone.Rq.Outfile_uuid)
	wone.Rq.Outfilename = temp_outfname
}

//Method to fill the output of write multi operation.
func (wml *zomatoRqOp) fillWm(wmul *writeMulti) {
	//Get timestamp.
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	wml.Operation = wmul.Rq.Operation
	wml.Timestamp = timestamp
	wml.Data = data
	status := strconv.Itoa(int(wml.Status))
	wr_mp := map[string]string{
		"Key":    wmul.Rq.Key,
		"Status": status,
	}
	//Fill write request data into a map.
	fillMap(wr_mp, wmul.Rq.Rncui)
}

//Method to fill the output of read one operation.
func (rof *zomatoRqOp) fillRo(rone *readOne) {
	//Get timestamp.
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	rof.Operation = rone.Rq.Operation
	rof.Timestamp = timestamp
	rof.Data = data
	//Dump structure into json.
	tmpout_filename := rof.dumpIntoJson(rone.Rq.Outfile_uuid)
	rone.Rq.Outfilename = tmpout_filename
}

//Method to fill the output of read multi operation.
func (rmf *zomatoRqOp) fillRm(zrm *readMulti) {
	//Get timestamp.
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	rmf.Operation = zrm.Rq.Operation
	rmf.Timestamp = timestamp
	rmf.Data = data
}

//Prepare method for write one operation to fill the required structure.
func (w *writeOne) Prepare() error {
	var err error
	rncui := w.Args[1]
	//Typecast Restaurant_id to int64.
	rest_id_string := w.Args[2]
	restaurant_id_str, err := strconv.ParseInt(rest_id_string, 10, 64)
	if err != nil {
		fmt.Println("Error occured in typecasting Restaurant_id to int64")
	}
	//Typecast Votes to int64.
	votes_str, err := strconv.ParseInt(w.Args[7], 10, 64)
	if err != nil {
		fmt.Println("Error occured in typecasting Votes to int64")
	}
	//Create a file for storing keys and rncui.
	os.Create("key_rncui_data.txt")
	//Fill the Zomato_App structure.
	cmddata := zomatoapplib.Zomato_Data{
		Restaurant_id:   restaurant_id_str,
		Restaurant_name: w.Args[3],
		City:            w.Args[4],
		Cuisines:        w.Args[5],
		Ratings_text:    w.Args[6],
		Votes:           votes_str,
	}
	w.Rq.Zomato_data = &cmddata
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
	if w.Rq.Zomato_data == nil {
		err = errors.New("Prepare method for WriteOne failed")
	}
	return err
}

//Exec method for write one operation which performs write operation.
func (woexc *writeOne) Exec() error {

	var error_msg error
	var wr_strdt_cmd zomatoRqOp
	//Perform write operation.
	err := woexc.Rq.Cli_obj.Write(woexc.Rq.Zomato_data, woexc.Args[1])
	if err != nil {
		fmt.Println("Write key-value failed : ", err)
		wr_strdt_cmd.Status = -1
		error_msg = errors.New("Exec method for WriteOne Operation failed.")
	} else {
		fmt.Println("Pmdb Write successful!")
		wr_strdt_cmd.Status = 0
		error_msg = nil
	}
	wr_strdt_cmd.fillWo(woexc)
	return error_msg
}

//Complete method for write one operation which creates final json outfile.
func (woc *writeOne) Complete() error {

	var cerr error
	//Copy contents in json outfile.
	err := copyToOutfile(woc.Rq.Outfilename, woc.Args[8])
	if err != nil {
		cerr = errors.New("Complete method for WriteOne Operation failed")
	}
	return cerr
}

//Prepare method for read one operation to fill the required structure.
func (rop *readOne) Prepare() error {

	var err error
	//Typecast key into int64.
	key_int64, _ := strconv.ParseInt(rop.Rq.Key, 10, 64)
	//Fill the Zomato_App structure.
	rdinfo := zomatoapplib.Zomato_Data{
		Restaurant_id: key_int64,
	}
	rop.Rq.Zomato_data = &rdinfo
	if rop.Rq.Zomato_data == nil {
		err = errors.New("Prepare method for ReadOne Operation failed")
	} else {
		err = nil
	}
	return err

}

//Exec method for read one operation to perform read operation.
func (roe *readOne) Exec() error {

	var roerr error
	//Perform read operation.
	rop := &zomatoapplib.Zomato_Data{}
	err := roe.Rq.Cli_obj.Read(roe.Rq.Zomato_data, roe.Rq.Rncui, rop)
	if err != nil {
		fmt.Println("Read request failed !!", err)
		rdt := &zomatoRqOp{Status: -1}
		rdt.fillRo(roe)
		roerr = errors.New("Exec method for ReadOne Operation failed")
	}
	roe.displayAndFill(rop)
	return roerr
}

//Complete method for read one operation which creates final json outfile.
func (roc *readOne) Complete() error {

	var cerr error
	//Copy contents in json outfile.
	err := copyToOutfile(roc.Rq.Outfilename, roc.Rq.Json_fname)
	if err != nil {
		cerr = errors.New("Complete method for ReadOne Operation failed")
	}
	return cerr
}

//Prepare method for write multi operation to fill the required structures.
func (wmp *writeMulti) Prepare() error {

	var wmperr error
	//Get array of zomato_data structure.
	mreqdata := wmp.getWmInfo()
	wmp.Multi_reqdata = mreqdata
	if wmp.Multi_reqdata == nil {
		wmperr = errors.New("Prepare method for WriteMulti Operation failed")
	} else {
		wmperr = nil
	}
	return wmperr
}

//Exec method for write multi operation which performs write operation.
func (wme *writeMulti) Exec() error {

	var excerr error
	var wr_strdata = &zomatoRqOp{}
	//Create a file for storing keys and rncui.
	os.Create("key_rncui_data.txt")
	//Open file for storing key, rncui in append mode.
	file, err := os.OpenFile("key_rncui_data.txt", os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}
	defer file.Close()
	for i := 0; i < len(wme.Multi_reqdata); i++ {
		//Generate app_uuid.
		app_uuid := uuid.NewV4().String()
		//Create rncui string.
		rncui := app_uuid + ":0:0:0:0"
		//Append key_rncui in file.
		rest_id_str := strconv.Itoa(int(wme.Multi_reqdata[i].Restaurant_id))
		_, err_write := file.WriteString("key, rncui = " + rest_id_str + "  " + rncui + "\n")
		if err_write != nil {
			log.Fatal(err)
		}
		wme.Rq.Key = rest_id_str
		wme.Rq.Rncui = rncui
		err := wme.Rq.Cli_obj.Write(wme.Multi_reqdata[i], rncui)
		if err != nil {
			fmt.Println("Pmdb Write failed.", err)
			wr_strdata.Status = -1
			excerr = errors.New("Exec method for WriteMulti Operation failed")
		} else {
			fmt.Println("Pmdb Write successful!")
			wr_strdata.Status = 0
			excerr = nil
		}
		wr_strdata.fillWm(wme)
	}
	//Dump structure into json.
	temp_outfname := wr_strdata.dumpIntoJson(wme.Rq.Outfile_uuid)
	wme.Rq.Outfilename = temp_outfname
	return excerr
}

//Complete method for write multi operation which creates final json outfile.
func (wmc *writeMulti) Complete() error {
	var cerr error
	//Copy contents in json outfile.
	err := copyToOutfile(wmc.Rq.Outfilename, wmc.Rq.Json_fname)
	if err != nil {
		cerr = errors.New("Complete method for WriteMulti Operation failed")
	}
	return cerr
}

///Prepare method for read multi operation which creates required structure.
func (rmp *readMulti) Prepare() error {
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
		rmp.Rm_rncui = rmrncui
		rmp.Rmdata = rmreq_dt
	}
	if rmp.Rmdata == nil && rmp.Rm_rncui == nil {
		prerr = errors.New("Prepare method for ReadMulti Operation failed")
	} else {
		prerr = nil
	}
	return prerr
}

//Exec method for read multi operation which performs read operation.
func (rme *readMulti) Exec() error {
	var rmexcerr error
	var rmdte = &zomatoRqOp{}
	if len(rme.Rmdata) == len(rme.Rm_rncui) {
		for i := range rme.Rmdata {
			//Perform read operation.
			rmop_struct := &zomatoapplib.Zomato_Data{}
			err := rme.Rq.Cli_obj.Read(rme.Rmdata[i], rme.Rm_rncui[i], rmop_struct)
			if err != nil {
				rmdte = &zomatoRqOp{Status: -1}
				rmdte.fillRm(rme)
				rmexcerr = errors.New("Exec method for ReadOne Operation failed")
			} else {
				rme.displayAndFill(rmdte, rmop_struct, i)
			}
		}
	}
	//Dump structure into json.
	temp_outfname := rmdte.dumpIntoJson(rme.Rq.Outfile_uuid)
	rme.Rq.Outfilename = temp_outfname
	return rmexcerr
}

//Complete method for read multi operation which creates final json outfile.
func (rmc *readMulti) Complete() error {
	var cerr error
	//Copy contents in json outfile.
	err := copyToOutfile(rmc.Rq.Outfilename, rmc.Rq.Json_fname)
	if err != nil {
		cerr = errors.New("Complete method for ReadMulti Operation failed")
	}
	return cerr
}

//Prepare method for get leader opeation which creates structure.
func (get_leader *getLeader) Prepare() error {

	var gleaerr error
	Pmdb_items := &PumiceDBCommon.PMDBInfo{
		RaftUUID:   raftUuid,
		ClientUUID: clientUuid,
	}
	get_leader.PmdbInfo = Pmdb_items

	if get_leader.PmdbInfo == nil {
		gleaerr = errors.New("Prepare method for get leader operation failed")
	} else {
		gleaerr = nil
	}
	return gleaerr
}

//Exec method for get leader operation.
func (get_leader *getLeader) Exec() error {

	var glexcerr error
	lea_uuid, err := get_leader.Rq.Cli_obj.PmdbGetLeader()
	if err != nil {
		get_leader.Rq.Status = -1
		fmt.Errorf("Failed to get Leader UUID")
	} else {
		get_leader.Rq.Status = 0
	}
	leader_uuid := lea_uuid.String()
	get_leader.PmdbInfo.LeaderUUID = leader_uuid

	if get_leader.PmdbInfo.LeaderUUID == "" {
		glexcerr = errors.New("Exec method for get leader operation failed")
	} else {
		fmt.Println("Leader uuid is:", get_leader.PmdbInfo.LeaderUUID)
		glexcerr = nil
	}
	return glexcerr
}

//Complete method for get leader operation which creates final json outfile.
func (get_leader *getLeader) Complete() error {

	var cerr error
	//Prepare json output filepath.
	json_outf := jsonOutFpath + "/" + get_leader.Rq.Json_fname + ".json"
	file, err := json.MarshalIndent(get_leader.PmdbInfo, "", "\t")
	err = ioutil.WriteFile(json_outf, file, 0644)

	if err != nil {
		cerr = errors.New("Complete method for get leader operation failed")
	} else {
		cerr = nil
	}
	return cerr
}

//Function to write write_data into map.
func fillMap(mp map[string]string, rncui string) {
	//Fill data into outer map.
	data[rncui] = mp
}

//Method to dump zomato_app_output structure into json file.
func (zj *zomatoRqOp) dumpIntoJson(outf_uuid string) string {
	//Prepare path for temporary json file.
	temp_outfile_name := jsonOutFpath + "/" + outf_uuid + ".json"
	file, _ := json.MarshalIndent(zj, "", "\t")
	_ = ioutil.WriteFile(temp_outfile_name, file, 0644)
	return temp_outfile_name
}

//Method to copy temporary json file into output json file.
func copyToOutfile(tmp_outfile_name, jsonfilename string) error {
	var errcp error
	//Prepare json output filepath.
	json_outf := jsonOutFpath + "/" + jsonfilename + ".json"
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
func getCmdParams() {
	flag.StringVar(&raftUuid, "r", "NULL", "raft uuid")
	flag.StringVar(&clientUuid, "u", "NULL", "client uuid")
	flag.StringVar(&jsonOutFpath, "l", "NULL", "json_outfilepath")

	flag.Parse()
	fmt.Println("Raft UUID: ", raftUuid)
	fmt.Println("Client UUID: ", jsonOutFpath)
	fmt.Println("Outfile path: ", jsonOutFpath)
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
	getCmdParams()
	//Create new client object.
	cli_obj := PumiceDBClient.PmdbClientNew(raftUuid, clientUuid)
	if cli_obj == nil {
		return
	}
	fmt.Println("Starting client: ", clientUuid)
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
		cmdS := strings.Replace(cmd, "\n", "", -1)
		opsSplit := strings.Split(cmdS, "#")
		ops := opsSplit[0]

		//Make the required maps.
		data = make(map[string]map[string]string)
		//Generate uuid for temporary json file.
		outfUuid := uuid.NewV4().String()
		//Declare interface variable.
		var zci ZomatoCli
		switch ops {
		case "WriteOne":
			zci = &writeOne{
				Args: opsSplit,
				Rq: &rqInfo{
					Outfile_uuid: outfUuid,
					Cli_obj:      cli_obj,
				},
			}
		case "ReadOne":
			zci = &readOne{
				Rq: &rqInfo{
					Key:          opsSplit[1],
					Rncui:        opsSplit[2],
					Json_fname:   opsSplit[3],
					Operation:    ops,
					Outfile_uuid: outfUuid,
					Cli_obj:      cli_obj,
				},
			}
		case "WriteMulti":
			zci = &writeMulti{
				Csv_fpath: opsSplit[1],
				Rq: &rqInfo{
					Operation:    ops,
					Json_fname:   opsSplit[2],
					Outfile_uuid: outfUuid,
					Cli_obj:      cli_obj,
				},
			}
		case "ReadMulti":
			zci = &readMulti{
				Rq: &rqInfo{
					Operation:    ops,
					Json_fname:   opsSplit[1],
					Outfile_uuid: outfUuid,
					Cli_obj:      cli_obj,
				},
			}
		case "get_leader":
			zci = &getLeader{
				Rq: &rqInfo{
					Operation:  ops,
					Json_fname: opsSplit[1],
					Cli_obj:    cli_obj,
				},
			}
		case "exit":
			os.Exit(0)
		default:
			fmt.Println("Enter valid operation: (WriteOne/ WriteMulti/ ReadOne/ ReadMulti/ get_leader /exit)")
			continue
		}
		//Perform Operations.
		prerr := zci.Prepare()
		if prerr != nil {
			log.Fatal(prerr)
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
