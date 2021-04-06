package main

import (
        "fmt"
        "os"
	"unsafe"
	"strings"
	"strconv"
        "gopmdblib/goPmdb"
        "zomatoapp/zomatoapplib"
)

/*
#cgo pkg-config: niova --define-variable=prefix=/usr/local/niova
#include <stdlib.h>
*/
import "C"

// Use the default column family
var colmfamily = "PMDBTS_CF"

func zomatoData_apply(app_id unsafe.Pointer, data_buf unsafe.Pointer,
                        data_buf_sz int64, pmdb_handle unsafe.Pointer){

	data := &zomatoapplib.Zomato_App{}
	PumiceDB.Decode(data_buf, data, data_buf_sz)
	fmt.Println("Data received from client: ", data)

	//Convert resturant_id from int to string and store as zomato_app_key.
	zomato_app_key := PumiceDB.GoIntToString(int(data.Restaurant_id))
	app_key_len := len(zomato_app_key)

	//Lookup for the key if it is already present.
	var previous_value string
        prev_data_value := PumiceDB.PmdbLookupKey(zomato_app_key, int64(app_key_len), previous_value, colmfamily)

	//If previous value is not null, update value of votes.
	if prev_data_value != ""{

		//Split the prev_data_value.
		res_data := strings.Split(prev_data_value, ".")

		//Take last parameter of res_data (votes) and convert it to int64.
		prev_votes,_ := strconv.ParseInt(res_data[len(res_data)-1], 10, 64)

		//Update votes by adding it with previous votes.
		data.Votes += prev_votes
	}


	//Convert votes from int to string.
        str_votes := PumiceDB.GoIntToString(int(data.Votes))

	zomato_app_value := data.Restaurant_name+"."+data.City+"."+data.Cuisines+"."+data.Ratings_text+"."+str_votes
        app_value_len := len(zomato_app_value)

	//Write key,values.
	PumiceDB.PmdbWriteKV(app_id, pmdb_handle, zomato_app_key, int64(app_key_len), zomato_app_value,
				 int64(app_value_len), colmfamily)
}

func zomatoData_read(app_id unsafe.Pointer, data_request_buf unsafe.Pointer,
            data_request_bufsz int64, data_reply_buf unsafe.Pointer, data_reply_bufsz int64) int64{

	fmt.Println("Read request received from client")

	//Decode the request structure sent by client.
	read_req_data := &zomatoapplib.Zomato_App{}

	fmt.Println("read_req_data: ",read_req_data)
	PumiceDB.Decode(data_request_buf, read_req_data, data_request_bufsz)

	fmt.Println("Key passed by client: ", read_req_data.Restaurant_id)

	/* Pass the work as key to PmdbReadKV and get the value from pumicedb */
	zapp_key := PumiceDB.GoIntToString(int(read_req_data.Restaurant_id))
	zapp_key_len := len(zapp_key)

	result := PumiceDB.PmdbReadKV(app_id, zapp_key, int64(zapp_key_len), colmfamily)

	//Split the result to get respective values.
	result_splt :=  strings.Split(result, ".")

	//Copy the result in data_reply_buf
	reply_data := (*zomatoapplib.Zomato_App)(data_reply_buf)
	reply_data.Restaurant_id = read_req_data.Restaurant_id
	reply_data.Restaurant_name = result_splt[0]
	reply_data.City = result_splt[1]
	reply_data.Cuisines = result_splt[2]
	reply_data.Ratings_text = result_splt[3]
	reply_data.Votes,_ = strconv.ParseInt(result_splt[4], 10, 64)

	fmt.Println("Zomato_data_app_key: ", reply_data.Restaurant_id)
	fmt.Println("Value for the respective key: "+reply_data.Restaurant_name+","+reply_data.City+","+reply_data.Cuisines+","+reply_data.Ratings_text+","+result_splt[4])

	return data_request_bufsz
}

//Get cmdline parameters and start server.
func Zomato_app_start_server(){

        raft_uuid := os.Args[1]
        peer_uuid := os.Args[2]

        fmt.Println("Raft uuid:", raft_uuid)
        fmt.Println("Peer uuid:", peer_uuid)

	//Initialize callbacks for zomato app.
        cb := &PumiceDB.PmdbCallbacks{
                ApplyCb: zomatoData_apply,
                ReadCb:  zomatoData_read,
        }

        PumiceDB.PmdbStartServer(raft_uuid, peer_uuid, colmfamily, cb)

}

func main(){
	fmt.Println("In main")

	Zomato_app_start_server()
}
