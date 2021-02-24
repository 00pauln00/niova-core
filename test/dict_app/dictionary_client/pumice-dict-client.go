package main
import (
	"fmt"
	"os"
	"unsafe"
	"bufio"
	"strings"
	"flag"
	"encoding/gob"
	"log"
	"gopmdblib/goPmdb"
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

type Dict_app struct {
	Dict_op string
	Dict_wr_seq uint64
	Dict_rncui string
	Dict_text string
	Dict_wcount int
}

func (pmdbDict Dict_app) PmdbEncode(encode *gob.Encoder) {

	err := encode.Encode(pmdbDict)
	if err != nil {
		log.Fatal(err)
	}
}

/*
 Start the pmdb client.
 Read the request from console and process it.
 User will pass the request in following format.
 app_uuid.Text.write => For write opertion.
 app_uuid.Word.write  => For read operation.
*/
func pmdbDictClient() {

	//Start the client.
	pmdb := GoPmdb.PmdbStartClient(raft_uuid_go, peer_uuid_go)

	var seq uint64
	for {
		//Read the input from console
		words := bufio.NewReader(os.Stdin)
		for {
			fmt.Print("Enter request in the format ")
			fmt.Print("app_uuid.text.write/read")
			text, _ := words.ReadString('\n')

			// convert CRLF to LF
			text = strings.Replace(text, "\n", "", -1)
			input := strings.Split(text, ".")
			seq++

			//Format is: AppUUID.text.write or AppUUID.text.read
			// Prepare the dictionary structure from values passed by user.
			input_dict := Dict_app{
				Dict_op: input[2],
				Dict_wr_seq: seq,
				Dict_rncui: input[0],
				Dict_text: input[1],
			}

			fmt.Println("rncui", input_dict.Dict_rncui)
			fmt.Println("Operation", input_dict.Dict_op)
			fmt.Println("text", input_dict.Dict_text)

			if input_dict.Dict_op == "write" {
				//write operation
				GoPmdb.PmdbClientWrite(input_dict, pmdb, input_dict.Dict_rncui)
			} else {
				//read operation
				var value_len int64
				value_ptr := GoPmdb.PmdbClientRead(input_dict, pmdb, input_dict.Dict_rncui, &value_len)

				go_value := unsafe.Pointer(value_ptr)
				//Decode the result from unsafe.Pointer to dictionary structure..
				result_dict := DictAppLib.DictAppDecodebuf(go_value, value_len)
				fmt.Println("Word: ", result_dict.Dict_text)
				fmt.Println("Frequecy of the word: ", result_dict.Dict_wcount)
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

	//Start pmdbDictionary client.
	pmdbDictClient()
}
