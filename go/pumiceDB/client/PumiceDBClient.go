package PumiceDBClient

import (
	"errors"
	"fmt"
	"niova/go-pumicedb-lib/common"
	"strconv"
	"syscall"
	"unsafe"
	"encoding/csv"
	"bufio"
	"strings"
	"os"
	"log"
	"io"
)

/*
#cgo LDFLAGS: -lniova -lniova_raft_client -lniova_pumice_client
#include <raft/pumice_db_client.h>
#include <raft/pumice_db_net.h>
*/
import "C"

type PmdbClientObj struct {
	initialized bool
	pmdb        C.pmdb_t
	raftUuid    string
	myUuid      string
	//Pmdb unsafe.Pointer
}

type CommonVars struct {
	Cli_obj PmdbClientObj
	Outfile_uuid string
	Json_filename string
}

//read cmdline input.
func get_cmdline_input(input []string) []string {

        //Read the key from console
        key := bufio.NewReader(os.Stdin)

        key_text, _ := key.ReadString('\n')

        // convert CRLF to LF
        key_text = strings.Replace(key_text, "\n", "", -1)
        input = strings.Split(key_text, "#")

        return input
}

func (cv_obj *CommonVars) GetInput(input []string) []string {
	return get_cmdline_input(input)
}

//parse csv file.
func parse_csv_file(filename string) (fp *csv.Reader) {

        // Open the file
        csvfile, err := os.Open(filename)
        if err != nil {
                log.Fatalln("Error to open the csv file", err)
        }

        // Skip first row (line)
        row1, err := bufio.NewReader(csvfile).ReadSlice('\n')
        if err != nil {
                log.Fatalln("error")
        }
        _, err = csvfile.Seek(int64(len(row1)), io.SeekStart)
        if err != nil {
                log.Fatalln("error")
        }
        // Parse the file
        fp = csv.NewReader(csvfile)

        return fp
}

func (cv_obj *CommonVars) ParseFile(filename string) (fp *csv.Reader) {
	return parse_csv_file(filename)
}

/* Typecast Go string to C String */
func GoToCString(gstring string) *C.char {
	return C.CString(gstring)
}

/* Free the C memory */
func FreeCMem(cstring *C.char) {
	C.free(unsafe.Pointer(cstring))
}

/* Typecast Go Int to string */
func GoIntToString(value int) string {
	return strconv.Itoa(value)
}

/* Type cast Go int64 to C size_t */
func GoToCSize_t(glen int64) C.size_t {
	return C.size_t(glen)
}

/* Typecast C size_t to Go int64 */
func CToGoInt64(cvalue C.size_t) int64 {
	return int64(cvalue)
}

/* Type cast C char * to Go string */
func CToGoString(cstring *C.char) string {
	return C.GoString(cstring)
}

//Write KV from client.
func (obj *PmdbClientObj) Write(ed interface{},
	rncui string) int {

	var key_len int64
	//Encode the structure into void pointer.
	ed_key := PumiceDBCommon.Encode(ed, &key_len)

	//Typecast the encoded key to char*
	encoded_key := (*C.char)(ed_key)

	//Perform the write
	return obj.writeKV(rncui, encoded_key, key_len)
}

//Read the value of key on the client
func (obj *PmdbClientObj) Read(ed interface{},
	rncui string,
	reply_size *int64) unsafe.Pointer {
	//Byte array
	fmt.Println("Client: Read Value for the given Key")

	var key_len int64
	//Encode the input buffer passed by client.
	ed_key := PumiceDBCommon.Encode(ed, &key_len)

	//Typecast the encoded key to char*
	encoded_key := (*C.char)(ed_key)

	return obj.readKV(rncui, encoded_key,
		key_len, reply_size)
}

func (obj *PmdbClientObj) GetLeader() string {

	leader_uuid := C.PmdbGetLeaderUUID(obj.pmdb)

	return C.GoString(leader_uuid)
}

func (obj *PmdbClientObj) writeKV(rncui string, key *C.char,
	key_len int64) int {

	var obj_stat C.pmdb_obj_stat_t

	crncui_str := GoToCString(rncui)
	defer FreeCMem(crncui_str)

	c_key_len := GoToCSize_t(key_len)

	var rncui_id C.struct_raft_net_client_user_id

	C.raft_net_client_user_id_parse(crncui_str, &rncui_id, 0)
	var obj_id *C.pmdb_obj_id_t

	obj_id = (*C.pmdb_obj_id_t)(&rncui_id.rncui_key)

	rc := C.PmdbObjPut(obj.pmdb, obj_id, key, c_key_len, &obj_stat)

	return int(rc)
}

func (obj *PmdbClientObj) readKV(rncui string, key *C.char,
	key_len int64,
	reply_size *int64) unsafe.Pointer {

	crncui_str := GoToCString(rncui)
	defer FreeCMem(crncui_str)

	c_key_len := GoToCSize_t(key_len)

	var rncui_id C.struct_raft_net_client_user_id

	C.raft_net_client_user_id_parse(crncui_str, &rncui_id, 0)
	var obj_id *C.pmdb_obj_id_t

	obj_id = (*C.pmdb_obj_id_t)(&rncui_id.rncui_key)

	var actual_value_size C.size_t

	reply_buff := C.PmdbObjGet(obj.pmdb, obj_id, key, c_key_len,
		&actual_value_size)

	*reply_size = int64(actual_value_size)

	fmt.Println("Reply size is: ", *reply_size)

	return reply_buff
}

// Return the decode / encode size of the provided object
func (obj *PmdbClientObj) GetSize(ed interface{}) int64 {
	return PumiceDBCommon.GetStructSize(ed)
}

// Decode in the input buffer into the output object
// XXX note this function *should* return an error
func (obj *PmdbClientObj) Decode(input unsafe.Pointer, output interface{},
	len int64) {
	PumiceDBCommon.Decode(input, output, len)
}

func (obj *PmdbClientObj) Stop() error {
	if obj.initialized == true {
		return errors.New("Client object is not initialized")
	}

	rc := C.PmdbClientDestroy((C.pmdb_t)(obj.pmdb))
	if rc != 0 {
		return fmt.Errorf("PmdbClientDestroy() returned %d", rc)
	}
	return nil
}

func (obj *PmdbClientObj) Start() error {
	if obj.initialized == true {
		return errors.New("Client object is already initialized")
	}

	raftUuid := GoToCString(obj.raftUuid)
	if raftUuid == nil {
		return errors.New("Memory allocation error")
	}
	defer FreeCMem(raftUuid)

	clientUuid := GoToCString(obj.myUuid)
	if clientUuid == nil {
		return errors.New("Memory allocation error")
	}
	defer FreeCMem(clientUuid)

	//Start the client
	obj.pmdb = C.PmdbClientStart(raftUuid, clientUuid)
	if obj.pmdb == nil {
		var errno syscall.Errno
		return fmt.Errorf("PmdbClientStart(): %d", errno)
	}

	return nil
}

func PmdbClientNew(Graft_uuid string, Gclient_uuid string) *PmdbClientObj {
	var client PmdbClientObj

	client.initialized = false
	client.raftUuid = Graft_uuid
	client.myUuid = Gclient_uuid

	return &client
}
