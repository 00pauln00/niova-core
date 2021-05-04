package main

import (
	"flag"
	"fmt"
	"gopmdblib/goPmdbCommon"
	"gopmdblib/goPmdbServer"
	"strconv"
	"strings"
	"unsafe"
	"zomatoapp.com/zomatolib"
)

/*
#include <stdlib.h>
#include <string.h>
*/
import "C"

var (
	raft_uuid string
	peer_uuid string
)

// Use the default column family
var colmfamily = "PMDBTS_CF"

func zomatodata_apply(app_id unsafe.Pointer, data_buf unsafe.Pointer,
	data_buf_sz int64, pmdb_handle unsafe.Pointer) {

	data := &zomatoapplib.Zomato_Data{}
	PumiceDBCommon.Decode(data_buf, data, data_buf_sz)
	fmt.Println("\nData received from client: ", data)

	//Convert resturant_id from int to string and store as zomato_app_key.
	zomato_app_key := strconv.Itoa(int(data.Restaurant_id))
	app_key_len := len(zomato_app_key)

	//Lookup for the key if it is already present.
	var previous_value string
	prev_data_value := PumiceDBServer.PmdbLookupKey(zomato_app_key, int64(app_key_len), previous_value, colmfamily)

	//If previous value is not null, update value of votes.
	if prev_data_value != "" {

		//Split the prev_data_value.
		res_data := strings.Split(prev_data_value, "_")

		//Take last parameter of res_data (votes) and convert it to int64.
		prev_votes, _ := strconv.ParseInt(res_data[len(res_data)-1], 10, 64)

		//Update votes by adding it with previous votes.
		data.Votes += prev_votes
	}

	//Convert votes from int to string.
	str_votes := strconv.Itoa(int(data.Votes))

	//Prepare string for zomato_app_value.
	zomato_app_value := data.Restaurant_name + "_" + data.City + "_" + data.Cuisines + "_" + data.Ratings_text + "_" + str_votes
	app_value_len := len(zomato_app_value)

	//Write key,values.
	PumiceDBServer.PmdbWriteKV(app_id, pmdb_handle, zomato_app_key, int64(app_key_len), zomato_app_value,
		int64(app_value_len), colmfamily)
}

func zomatodata_read(app_id unsafe.Pointer, data_request_buf unsafe.Pointer,
	data_request_bufsz int64, data_reply_buf unsafe.Pointer, data_reply_bufsz int64) int64 {

	fmt.Println("\nRead request received from client")

	//Decode the request structure sent by client.
	read_req_data := &zomatoapplib.Zomato_Data{}

	PumiceDBCommon.Decode(data_request_buf, read_req_data, data_request_bufsz)

	fmt.Println("Key passed by client: ", read_req_data.Restaurant_id)

	//Typecast Restaurant_id into string.
	zapp_key := strconv.Itoa(int(read_req_data.Restaurant_id))
	zapp_key_len := len(zapp_key)

	result := PumiceDBServer.PmdbReadKV(app_id, zapp_key, int64(zapp_key_len), colmfamily)

	//Split the result to get respective values.
	result_splt := strings.Split(result, "_")

	votes_int64, _ := strconv.ParseInt(result_splt[4], 10, 64)
	//Copy the result in data_reply_buf
	reply_data := zomatoapplib.Zomato_Data{
		Restaurant_id:   read_req_data.Restaurant_id,
		Restaurant_name: result_splt[0],
		City:            result_splt[1],
		Cuisines:        result_splt[2],
		Ratings_text:    result_splt[3],
		Votes:           votes_int64,
	}

	//Copy the encoded result in reply_buffer
	data_reply_size := PumiceDBServer.PmdbCopyDataToBuffer(reply_data, data_reply_buf)
	fmt.Println("length of buffer is:", data_reply_size)
	return data_reply_size
}

func zomato_app_start_server() {

	//Method call to accept cmdline parameters and start server.
	flag.StringVar(&raft_uuid, "r", "NULL", "raft uuid")
	flag.StringVar(&peer_uuid, "u", "NULL", "peer uuid")

	flag.Parse()
	fmt.Println("Raft UUID: ", raft_uuid)
	fmt.Println("Peer UUID: ", peer_uuid)

	//Initialize callbacks for zomato app.
	cb := &PumiceDBServer.PmdbCallbacks{
		ApplyCb: zomatodata_apply,
		ReadCb:  zomatodata_read,
	}

	//Start pumicedb server.
	PumiceDBServer.PmdbStartServer(raft_uuid, peer_uuid, colmfamily, cb)

}

func main() {
	//Start zomato_app_server.
	zomato_app_start_server()
}
