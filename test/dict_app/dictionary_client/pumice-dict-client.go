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
	"flag"
	"dictapplib/dict_libs"
)

/*
#cgo pkg-config: niova --define-variable=prefix=/home/manisha/binaries
#include <raft/pumice_db.h>
#include <raft/pumice_db_client.h>
#include <rocksdb/c.h>
#include <raft/raft_net.h>
#include <raft/pumice_db_net.h>
*/
import "C"

var raft_uuid_go string
var peer_uuid_go string

func GoDictClient() {

	//XXX move this the golibrary
	raft_uuid := C.CString(raft_uuid_go)
	defer C.free(unsafe.Pointer(raft_uuid))

	peer_uuid := C.CString(peer_uuid_go)
	defer C.free(unsafe.Pointer(peer_uuid))

	//Start the client.
	var Cpmdb C.pmdb_t
	Cpmdb  = C.PmdbClientStart(raft_uuid, peer_uuid)

	var seq uint64
	for {
		//Read the input from console
		words := bufio.NewReader(os.Stdin)
		for {
			text, _ := words.ReadString('\n')

			// convert CRLF to LF
			text = strings.Replace(text, "\n", "", -1)
			input := strings.Split(text, ".")
			seq++

			//Format is: AppUUID.text.write or AppUUID.text.read
			// Prepare the dictionary structure from values passed by user.
			input_dict := DictAppLib.Dict_request{
				Dict_op: input[2],
				Dict_wr_seq: seq,
				Dict_rncui: input[0],
				Dict_text: input[1],
			}

			var obj_stat C.pmdb_obj_stat_t

			crncui := C.CString(input[0])
			defer C.free(unsafe.Pointer(crncui))

			// XXX Get the size of byte array, need to get from DictAppEncodebuf
			var request_size int64

			//Encode the Dictionary structure into void * pointer for passing to C function.
			request_ptr := DictAppLib.DictAppEncodebuf(input_dict, &request_size)

			if input_dict.Dict_op == "write" {

				//Send the write request
				//XXX Lets not call cfunction directly from application. Create library
				//function for it.
				C.PmdbObjPutGolang(Cpmdb, crncui, request_ptr, request_size, &obj_stat)

			} else {

				//XXX same here. This should happen from Golibrary.
				C.PmdbObjGetXGolang(Cpmdb, crncui, request_ptr, request_size,
									result_ptr, size, &obj_stat)
			}
		}
	}
}

func pmdb_dict_app_getopts() {

	flag.StringVar(&raft_uuid_go, "raft", "NULL", "raft uuid")
	flag.StringVar(&peer_uuid_go, "peer", "NULL", "peer uuid")

	flag.Parse()
	fmt.Println("Raft UUID: ", raft_uuid_go)
	fmt.Println("Peer UUID: ", peer_uuid_go)
}

func main() {
	//Parse the cmdline parameter
	pmdb_dict_app_getopts()

	GoDictClient()
}
