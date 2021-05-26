package main

import (
	"dictapplib/lib"
	"flag"
	"fmt"
	"log"
	"niova/go-pumicedb-lib/server"
	"strconv"
	"strings"
	"unsafe"
)

/*
#include <string.h>
*/
import "C"

var seqno = 0
var raft_uuid_go string
var peer_uuid_go string
var word_map map[string]int

// Use the default column family
var colmfamily = "PMDBTS_CF"

type DictionaryServer struct {
	raftUuid       string
	peerUuid       string
	columnFamilies string
	pso            *PumiceDBServer.PmdbServerObject
}

func main() {
	//Parse the cmdline parameters and generate new Dictionary object
	dso := dictionaryServerNew()

	/*
		Initialize the internal pmdb-server-object pointer.
		Assign the Directionary object to PmdbAPI so the apply and
		read callback functions can be called through pmdb common library
		functions.
	*/
	dso.pso = &PumiceDBServer.PmdbServerObject{
		ColumnFamilies: colmfamily,
		RaftUuid:       dso.raftUuid,
		PeerUuid:       dso.peerUuid,
		PmdbAPI:        dso,
	}

	//Create empty word map
	word_map = make(map[string]int)

	// Start the pmdb server
	err := dso.pso.Run()

	if err != nil {
		log.Fatal(err)
	}
}

func dictionaryServerNew() *DictionaryServer {
	dso := &DictionaryServer{}

	flag.StringVar(&dso.raftUuid, "r", "NULL", "raft uuid")
	flag.StringVar(&dso.peerUuid, "u", "NULL", "peer uuid")

	flag.Parse()
	fmt.Println("Raft UUID: ", dso.raftUuid)
	fmt.Println("Peer UUID: ", dso.peerUuid)

	return dso
}

//split the string and add each word in the word-map
func split_and_write_to_word_map(text string) {
	words := strings.Fields(text)
	// Store words and its count in the map
	for _, word := range words {
		word_map[word]++
	}
}

func (dso *DictionaryServer) Apply(app_id unsafe.Pointer,
	input_buf unsafe.Pointer, input_buf_sz int64,
	pmdb_handle unsafe.Pointer) {

	fmt.Println("Dictionary app server: Apply request received")

	/* Decode the input buffer into dictionary structure format */
	apply_dict := &DictAppLib.Dict_app{}
	dso.pso.Decode(input_buf, apply_dict, input_buf_sz)

	fmt.Println("Key passed by client: %s", apply_dict.Dict_text)

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
		prev_result := dso.pso.LookupKey(word, int64(go_key_len), prev_value,
			colmfamily)
		fmt.Println("Previous value of the key: ", prev_result)

		if prev_result != "" {
			//Convert the word count into string.
			prev_result_int, _ := strconv.Atoi(prev_result)
			count = count + prev_result_int
		}

		fmt.Println("Now the Frequency of the word is: ", count)
		value := PumiceDBServer.GoIntToString(count)
		value_len := PumiceDBServer.GoStringLen(value)

		fmt.Println("Write the KeyValue by calling PmdbWriteKV")
		//Write word and frequency as value to Pmdb
		dso.pso.WriteKV(app_id, pmdb_handle, word, int64(go_key_len), value,
			int64(value_len), colmfamily)

		//Delete the word entry once written in the pumicedb
		delete(word_map, word)
	}
}

func (dso *DictionaryServer) Read(app_id unsafe.Pointer,
	request_buf unsafe.Pointer, request_bufsz int64,
	reply_buf unsafe.Pointer, reply_bufsz int64) int64 {

	fmt.Println("Dictionary App: Read request received")

	//Decode the request structure sent by client.
	req_dict := &DictAppLib.Dict_app{}
	dso.pso.Decode(request_buf, req_dict, request_bufsz)

	fmt.Println("Key passed by client: %s", req_dict.Dict_text)

	key_len := len(req_dict.Dict_text)

	/* Pass the work as key to PmdbReadKV and get the value from pumicedb */
	result := dso.pso.ReadKV(app_id, req_dict.Dict_text, int64(key_len), colmfamily)

	/* typecast the output to int */
	word_frequency := 0
	if result != "" {
		word_count, err := strconv.Atoi(result)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("Frequency of the word is: ", word_count)
		word_frequency = word_count
	}

	result_dict := DictAppLib.Dict_app{
		Dict_text:   req_dict.Dict_text,
		Dict_wcount: word_frequency,
	}

	//Copy the encoded result in reply_buffer
	reply_size := dso.pso.CopyDataToBuffer(result_dict, reply_buf)

	fmt.Println("Reply size is: ", reply_size)
	return reply_size
}
