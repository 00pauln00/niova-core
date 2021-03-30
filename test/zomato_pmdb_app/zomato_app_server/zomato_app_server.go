package main

import (
        "fmt"
        "os"
	"unsafe"
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
	fmt.Println("In Apply....Data received from client: ", data)

	//Convert votes in int to string.
	str_votes := PumiceDB.GoIntToString(int(data.Votes))

	//Convert resturant_id from int to string as store as zomato_app_key.
	zomato_app_key := PumiceDB.GoIntToString(int(data.Restaurant_id))

	app_key_len := len(zomato_app_key)

	zomato_app_value := data.Restaurant_name+","+data.City+","+data.Cuisines+","+data.Ratings_text+","+str_votes

	app_value_len := len(zomato_app_value)

	//Write key,values.
	PumiceDB.PmdbWriteKV(app_id, pmdb_handle, zomato_app_key, int64(app_key_len), zomato_app_value,
				 int64(app_value_len), colmfamily)
}

func zomatoData_read(app_id unsafe.Pointer, data_request_buf unsafe.Pointer,
            data_request_bufsz int64, data_reply_buf unsafe.Pointer, data_reply_bufsz int64) int64{
	fmt.Println("In read")
	return 0
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
