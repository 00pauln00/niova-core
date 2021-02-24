package main
import (
	"fmt"
	"unsafe"
	"strconv"
	"strings"
	"flag"
	"gopmdblib/goPmdb"
	"dictapplib/dict_libs"
)

/*
#cgo pkg-config: niova --define-variable=prefix=/usr/local/niova
#include <raft/pumice_db.h>
#include <rocksdb/c.h>
#include <raft/raft_net.h>
*/
import "C" //There should be no empty line between above c declarations and import "c"

var seqno = 0
var raft_uuid_go string
var peer_uuid_go string
var word_map map[string]int
// Use the default column family 
var colmfamily = "PMDBTS_CF"

//split the string and add each word in the word-map
func split_and_write_to_word_map(text string) {
	words := strings.Fields(text)
	// Store words and its count in the map
    for _,word := range words{
        word_map[word]++
    }
}

func dict_apply(app_id unsafe.Pointer, input_buf unsafe.Pointer,
			input_buf_sz int64, pmdb_handle unsafe.Pointer) {
	fmt.Println("Apply request received")

	/* Decode the input buffer into dictionary structure format */
	apply_dict := DictAppLib.DictAppDecodebuf(input_buf, input_buf_sz)

	fmt.Println("Operation type %s", apply_dict.Dict_op)
	fmt.Println("Wr seq type %d", apply_dict.Dict_wr_seq)
	fmt.Println("rncui type %s", apply_dict.Dict_rncui)
	fmt.Println("Text %s", apply_dict.Dict_text)

	/* Split the words and create map for word to frequency */
	split_and_write_to_word_map(apply_dict.Dict_text)

	/*
     	Iterate over word_map and write work as key and frequency
	 as value to pmdb.
	*/
	for word, count := range word_map {
		go_key_len := len(word)
		var prev_value string

		//Lookup the key first
		prev_result := GoPmdb.PmdbLookupKey(app_id, word, int64(go_key_len), prev_value, colmfamily)
		fmt.Println("Previous value of the key: ", prev_result)
		//Convert the word count into string.
		prev_result_int, _ := strconv.Atoi(prev_result)
		count = count + prev_result_int
		fmt.Println("Now the count becomes: ", count)
		value := strconv.Itoa(count)

		value_len := len(value)

		GoPmdb.PmdbWriteKV(app_id, pmdb_handle, word, int64(go_key_len), value,
				 int64(value_len), colmfamily)

		//Delete the word entry once written in the pumicedb
		delete(word_map, word)
	}
}

func dict_read(app_id unsafe.Pointer, request_buf unsafe.Pointer,
            request_bufsz int64, reply_buf unsafe.Pointer, reply_bufsz int64) {
	fmt.Println("Read request received")

	read_dict := DictAppLib.DictAppDecodebuf(request_buf, request_bufsz)

	fmt.Println("dict_read: Operation type %s", read_dict.Dict_op)
	fmt.Println("dict_read: Wr seq type %d", read_dict.Dict_wr_seq)
	fmt.Println("dict_read: rncui type %s", read_dict.Dict_rncui)
	fmt.Println("dict_read: Text %s", read_dict.Dict_text)

	key_len := len(read_dict.Dict_text)

	GoPmdb.PmdbReadKV(app_id, read_dict.Dict_text, int64(key_len), request_buf,
					request_bufsz, colmfamily)
}

func pmdb_dict_app_getopts() {

	flag.StringVar(&raft_uuid_go, "raft", "NULL", "raft uuid")
	flag.StringVar(&peer_uuid_go, "peer", "NULL", "peer uuid")

	flag.Parse()
	fmt.Println("Raft UUID: ", raft_uuid_go)
	fmt.Println("Peer UUID: ", peer_uuid_go)
}

func main() {
	//Parse the cmdline parameters
	pmdb_dict_app_getopts()

	//Create empty word map
	word_map = make(map[string]int)

	//Initialize the dictionary application callback functions
	cb := &GoPmdb.PmdbCallbacks{
		ApplyCb: dict_apply,
		ReadCb:  dict_read,
	}

	GoPmdb.PmdbStartServer(raft_uuid_go, peer_uuid_go, colmfamily, cb)
}
