package main

import (
	"flag"
	"fmt"
	"foodpalaceapp.com/foodpalaceapplib"
	log "github.com/sirupsen/logrus"
	"niova/go-pumicedb-lib/server"
	"os"
	"strconv"
	"strings"
	"unsafe"
)

/*
#include <stdlib.h>
#include <string.h>
*/
import "C"

var (
	raftUuid string
	peerUuid string
	//Use the default column family
	colmfamily = "PMDBTS_CF"
	logDir     string
)

type FoodpalaceServer struct {
	raftUuid       string
	peerUuid       string
	columnFamilies string
	pso            *PumiceDBServer.PmdbServerObject
}

//Method to initizalize logger.
func (fpso *FoodpalaceServer) initLogger() {

	var filename string = logDir + "/" + fpso.peerUuid + ".log"
	// Create the log file if doesn't exist. And append to it if it already exists.
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	Formatter := new(log.TextFormatter)

	//Set Formatter.
	Formatter.TimestampFormat = "02-01-2006 15:04:05"
	Formatter.FullTimestamp = true
	log.SetFormatter(Formatter)

	if err != nil {
		// Cannot open log file. Logging to stderr
		fmt.Println(err)
	} else {
		log.SetOutput(f)
	}
	log.Info("peer:", fpso.peerUuid)
}

//Method for Apply callback.
func (fpso *FoodpalaceServer) Apply(app_id unsafe.Pointer, data_buf unsafe.Pointer,
	data_buf_sz int64, pmdb_handle unsafe.Pointer) {

	data := &foodpalaceapplib.FoodpalaceData{}
	fpso.pso.Decode(data_buf, data, data_buf_sz)
	log.Info("Data received from client: ", data)

	//Convert resturant_id from int to string and store as fp_app_key.
	fp_app_key := strconv.Itoa(int(data.Restaurant_id))
	app_key_len := len(fp_app_key)

	//Lookup for the key if it is already present.
	prev_data_value, err := fpso.pso.LookupKey(fp_app_key, int64(app_key_len), colmfamily)

	//If previous value is not null, update value of votes.
	if err == nil {

		//Split the prev_data_value.
		res_data := strings.Split(prev_data_value, "_")

		//Take last parameter of res_data (votes) and convert it to int64.
		prev_votes, _ := strconv.ParseInt(res_data[len(res_data)-1], 10, 64)

		//Update votes by adding it with previous votes.
		data.Votes += prev_votes
	}

	//Convert votes from int to string.
	str_votes := strconv.Itoa(int(data.Votes))

	//Prepare string for fp_app_value.
	fp_app_value := data.Restaurant_name + "_" + data.City + "_" + data.Cuisines + "_" + data.Ratings_text + "_" + str_votes
	app_value_len := len(fp_app_value)

	//Write key,values.
	fpso.pso.WriteKV(app_id, pmdb_handle, fp_app_key, int64(app_key_len), fp_app_value,
		int64(app_value_len), colmfamily)
}

//Method for read callback.
func (fpso *FoodpalaceServer) Read(app_id unsafe.Pointer, data_request_buf unsafe.Pointer,
	data_request_bufsz int64, data_reply_buf unsafe.Pointer, data_reply_bufsz int64) int64 {

	var result_splt []string
	log.Info("Read request received from client")

	//Decode the request structure sent by client.
	read_req_data := &foodpalaceapplib.FoodpalaceData{}

	fpso.pso.Decode(data_request_buf, read_req_data, data_request_bufsz)

	log.Info("Key passed by client: ", read_req_data.Restaurant_id)

	//Typecast Restaurant_id into string.
	zapp_key := strconv.Itoa(int(read_req_data.Restaurant_id))
	zapp_key_len := len(zapp_key)

	result, read_err := fpso.pso.ReadKV(app_id, zapp_key, int64(zapp_key_len), colmfamily)
	if read_err == nil {
		//Split the result to get respective values.
		result_splt = strings.Split(result, "_")
	}

	votes_int64, _ := strconv.ParseInt(result_splt[4], 10, 64)
	//Copy the result in data_reply_buf.
	reply_data := foodpalaceapplib.FoodpalaceData{
		Restaurant_id:   read_req_data.Restaurant_id,
		Restaurant_name: result_splt[0],
		City:            result_splt[1],
		Cuisines:        result_splt[2],
		Ratings_text:    result_splt[3],
		Votes:           votes_int64,
	}

	//Copy the encoded result in reply_buffer.
	data_reply_size, copy_err := fpso.pso.CopyDataToBuffer(reply_data, data_reply_buf)
	if copy_err != nil {
		log.Fatal("Failed to Copy result in the buffer: %s", copy_err)
	}
	log.Info("length of buffer is:", data_reply_size)
	return data_reply_size
}

//Function to get commandline parameters and initizalize FoodpalaceServer instance.
func foodPalaceServerNew() *FoodpalaceServer {

	fpso := &FoodpalaceServer{}

	//Method call to accept cmdline parameters and start server.
	flag.StringVar(&fpso.raftUuid, "r", "NULL", "raft uuid")
	flag.StringVar(&fpso.peerUuid, "u", "NULL", "peer uuid")
	flag.StringVar(&logDir, "l", "./", "log directory path")
	flag.Parse()

	log.Info("Raft UUID: ", fpso.raftUuid)
	log.Info("Peer UUID: ", fpso.peerUuid)
	log.Info("Log Directory Path:", logDir)

	return fpso
}

//If log directory is not exist it creates directory.
//and if dir path is not passed then it will create log file in current directory by default.
func makeDirectoryIfNotExists() error {

	if _, err := os.Stat(logDir); os.IsNotExist(err) {
		return os.Mkdir(logDir, os.ModeDir|0755)
	}
	return nil
}

func main() {

	//Print help message.
	if len(os.Args) == 1 || os.Args[1] == "-help" || os.Args[1] == "--help" || os.Args[1] == "-h" {
		fmt.Println("\nUsage: \n   For help:             ./foodpalaceappserver [-h] \n   To start server:      ./foodpalaceappserver -r [raft_uuid] -u [peer_uuid]")
		fmt.Println("\nPositional Arguments: \n   -r    raft_uuid \n   -u    peer_uuid")
		fmt.Println("\nOptional Arguments: \n   -h, --help            show this help message and exit")
		os.Exit(0)
	}

	//Get Command line parameters and create FoodpalaceServer structure instance.
	fpso := foodPalaceServerNew()

	//Create log directory if not exists.
	makeDirectoryIfNotExists()

	//Initialize logger.
	fpso.initLogger()

	/*
	   Initialize the internal pmdb-server-object pointer.
	   Assign the Directionary object to PmdbAPI so the apply and
	   read callback functions can be called through pmdb common library
	   functions.
	*/
	fpso.pso = &PumiceDBServer.PmdbServerObject{
		ColumnFamilies: colmfamily,
		RaftUuid:       fpso.raftUuid,
		PeerUuid:       fpso.peerUuid,
		PmdbAPI:        fpso,
	}

	//Start the pmdb server.
	err := fpso.pso.Run()

	if err != nil {
		log.Fatal(err)
	}
}
