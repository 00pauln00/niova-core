package main
import (
	"bytes"
	"fmt"
	"os"
	"unsafe"
	"bufio"
	"strings"
	"encoding/gob"
	"encoding/binary"
	"log"
)

/*
#cgo pkg-config: niova --define-variable=prefix=/home/manisha/binaries/niova/
#include <raft/pumice_db.h>
#include <raft/pumice_db_client.h>
#include <rocksdb/c.h>
#include <raft/raft_net.h>
#include <raft/pumice_db_net.h>
*/
import "C"

type dict_request struct {
	Dict_op string
	Dict_wr_seq uint64
	Dict_rncui string
	Dict_text string
}

func GoTraverse() {
	fmt.Println("Inside GoTraverse")
	//First parameter is RAFT_UUID
	ruuid := os.Args[1]

    fmt.Printf("Raft uuid: %s\n", ruuid)
	raft_uuid := C.CString(ruuid)
	defer C.free(unsafe.Pointer(raft_uuid))

	//Second parameter is PEER_UUID
	puuid := os.Args[2]

    fmt.Printf("Peer uuid: %s\n", puuid)
	peer_uuid := C.CString(puuid)
	defer C.free(unsafe.Pointer(peer_uuid))

	var Cpmdb C.pmdb_t
	Cpmdb  = C.PmdbClientStart(raft_uuid, peer_uuid)

	var seq uint64
	for {
		//Read the input from console
		words := bufio.NewReader(os.Stdin)
		for {
			fmt.Print("-> ")
			text, _ := words.ReadString('\n')

			// convert CRLF to LF
			text = strings.Replace(text, "\n", "", -1)
			input := strings.Split(text, ".")
			fmt.Println("RNCUI", input[0])
			fmt.Println("Text", input[1])
			fmt.Println("Operation", input[2])
			seq++

			my_dict := dict_request{
				Dict_op: input[2],
				Dict_wr_seq: seq,
				Dict_rncui: input[0],
				Dict_text: input[1],
			}


			//req_ptr := dict_request{dict_op:input[2], dict_wr_seq:seq, dict_text:input[1], dict_rncui:input[0]}

			var obj_stat C.pmdb_obj_stat_t
			//var request_rncui C.struct_raft_net_client_user_id
			//var rncui C.struct_raft_net_client_user_id

			fmt.Println("COnvert the string to c string")
			crncui := C.CString(input[0])
			defer C.free(unsafe.Pointer(crncui))

			//struct_ptr := unsafe.Pointer(my_dict)
			//defer C.free(unsafe.Pointer(struct_ptr))

			buf := bytes.Buffer{}
			enc := gob.NewEncoder(&buf)
			err := enc.Encode(my_dict)
			if err != nil {
				log.Fatal(err)
			}

			request := buf.Bytes()
			fmt.Println("Passing byte array: %s", string(request))

			request_ptr := unsafe.Pointer(&request[0])
			defer C.free(unsafe.Pointer(request_ptr))

			// Get the size of byte array
			size := C.size_t(binary.Size(request))
			fmt.Println("Calling c function")
			if my_dict.Dict_op == "write" {
				C.PmdbObjPutGolang(Cpmdb, crncui, request_ptr, size, &obj_stat)
			} else {
				var result dict_request
				res_buf := bytes.Buffer{}
				enc := gob.NewEncoder(&res_buf)
				err := enc.Encode(result)
				if err != nil {
					log.Fatal(err)
				}

				ptr := buf.Bytes()
				result_ptr := unsafe.Pointer(&ptr[0])
				defer C.free(unsafe.Pointer(result_ptr))

				C.PmdbObjGetXGolang(Cpmdb, crncui, request_ptr, size, result_ptr, size, &obj_stat)
			}
		}
	}
}

func main() {
	fmt.Println("Inside go main")
	GoTraverse()
}
