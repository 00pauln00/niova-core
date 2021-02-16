package main
import (
	"fmt"
	"io"
	"unsafe"
	"strconv"
	"encoding/gob"
	"log"
	"bytes"
	"strings"
	"flag"
	"gopmdblib/goPmdb"
	"dictapplib/dict_libs"
)

/*
#cgo pkg-config: niova --define-variable=prefix=/home/manisha/binaries
#include <raft/pumice_db.h>
#include <rocksdb/c.h>
#include <raft/raft_net.h>
*/
import "C" //There should be no empty line between above c declarations and import "c"
import gopointer "github.com/mattn/go-pointer"

var seqno = 0
var raft_uuid_go string
var peer_uuid_go string
var word_map map[string]int
// Use the default column family 
var colmfamily = "PMDBTS_CF"

/*
type dict_request struct {
	Dict_op string
	Dict_wr_seq uint64
	Dict_rncui string
	Dict_text string
}
*/

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
	fmt.Println("from go myapply")

	/*
		- Convert the void* input_buf i.e (input_buf unsafe.Pointer) to
		dict_struct structure.
		- First convert the unsafe.Pointer to Bytes Array with specifying
		input_buf_size.
		- Then Decode the byte array to dict_request structure.
	*/

	/*
	gob.Register(dict_request{})
	input_bytes_data := C.GoBytes(unsafe.Pointer(input_buf), C.int(input_buf_sz))

	input_buffer := bytes.NewBuffer(input_bytes_data)
	fmt.Println("New buffer %s", input_buffer)

	dict_req := &dict_request{}
	dec := gob.NewDecoder(input_buffer)
	for {
		if err := dec.Decode(dict_req); err == io.EOF {
			fmt.Println("EOF reached, break from the loop")
			break
		} else if err != nil {
			log.Fatal(err)
		}
	}
	*/

	dict_req := DictAppLib.DictAppDecodebuf(input_buf, input_buf_sz)

	fmt.Println("Operation type %s", dict_req.Dict_op)
	fmt.Println("Wr seq type %d", dict_req.Dict_wr_seq)
	fmt.Println("rncui type %s", dict_req.Dict_rncui)
	fmt.Println("Text %s", dict_req.Dict_text)

	split_and_write_to_word_map(dict_req.Dict_text)

	// Iterate over word_map and write work as key and frequency as value to pmdb.
	for word, count := range word_map {
		go_key_len := len(word)

		//Convert the word count into string.
		value := strconv.Itoa(count)

		value_len := len(value)

		GoPmdb.GoWriteKV(app_id, pmdb_handle, word, int64(go_key_len), value,
						 int64(value_len), colmfamily)
	}
}

func dict_read(app_id unsafe.Pointer, request_buf unsafe.Pointer,
            request_bufsz int64, reply_buf unsafe.Pointer, reply_bufsz int64) {
	fmt.Println("from go myread")

	recv_data := C.GoBytes(unsafe.Pointer(request_buf), C.int(request_bufsz))
	fmt.Println("Convert the bytes arr %s", string(recv_data))

	recv_bytes_arr := bytes.NewBuffer(recv_data)
	fmt.Println("New buffer %s", recv_bytes_arr)

	request := &DictAppLib.Dict_request{}
	dec := gob.NewDecoder(recv_bytes_arr)
	for {
		if err := dec.Decode(request); err == io.EOF {
			fmt.Println("EOF reached, break from the loop")
			break
		} else if err != nil {
			log.Fatal(err)
		}
	}

	fmt.Println("Operation type %s", request.Dict_op)
	fmt.Println("Wr seq type %d", request.Dict_wr_seq)
	fmt.Println("rncui type %s", request.Dict_rncui)
	fmt.Println("Text %s", request.Dict_text)

	key_len := len(request.Dict_text)

	GoPmdb.GoReadKV(app_id, request.Dict_text, int64(key_len), request_buf,
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
	cb := &GoPmdb.GoPmdbCallbacks{
		ApplyCb: dict_apply,
		ReadCb:  dict_read,
	}

	// Create an opaque C pointer for cbs to pass to GoStartServer.
	opa_ptr:= gopointer.Save(cb)
	defer gopointer.Unref(opa_ptr)

	GoPmdb.GoStartServer(raft_uuid_go, peer_uuid_go, opa_ptr)
}
