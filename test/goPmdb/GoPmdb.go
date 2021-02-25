package GoPmdb
import (
	"fmt"
	"unsafe"
	"encoding/gob"
	"bytes"
	"reflect"
)

/*
#cgo pkg-config: niova --define-variable=prefix=/usr/local/niova
#include <raft/pumice_db.h>
#include <rocksdb/c.h>
#include <raft/raft_net.h>
#include <raft/pumice_db_client.h>
extern void applyCgo(const struct raft_net_client_user_id *, const void *,
                     size_t, void *, void *);
extern void readCgo(const struct raft_net_client_user_id *, const void *,
                    size_t, void *, size_t, void *);
*/
import "C"

import gopointer "github.com/mattn/go-pointer"

type PmdbApplyCallback func(unsafe.Pointer, unsafe.Pointer, int64,
                          unsafe.Pointer)
type PmdbReadCallback func(unsafe.Pointer, unsafe.Pointer, int64,
                         unsafe.Pointer, int64)

//Callback function for pmdb apply and read
type PmdbCallbacks struct {
	ApplyCb PmdbApplyCallback
	ReadCb PmdbReadCallback
}

type charsSlice []*C.char

//Generic encoder/decoder interface
type PmdbEncDec interface {
	PmdbEncode(enc *gob.Encoder)
}
type cfSlice []*C.char

func GoToCString(gstring string) *C.char {
	cstring := C.CString(gstring)
	defer C.free(unsafe.Pointer(cstring))

	return cstring
}

func GoToCSize_t(glen int64) C.size_t {
	clen := C.size_t(glen)
	return clen
}

func CToGoInt64(cvalue C.size_t) int64 {
	gvalue := int64(cvalue)
	return gvalue
}

func CToGoString(cstring *C.char) string {
	gstring := C.GoString(cstring)
	return gstring
}

func Encode(ed PmdbEncDec, data_len *int64) *C.char {
	fmt.Println("Client: Write Key-Value")
	//Byte array
	buffer := bytes.Buffer{}

	encode := gob.NewEncoder(&buffer)
	ed.PmdbEncode(encode)

	struct_data := buffer.Bytes()
	*data_len = int64(len(struct_data))

	//Convert it to unsafe pointer (void * for C function) 
	enc_data := (*C.char)(unsafe.Pointer(&struct_data[0]))

	return enc_data
}

//Write KV from client.
func PmdbClientWrite(ed PmdbEncDec, pmdb unsafe.Pointer, rncui string) {

	fmt.Println("Client: Write Key-Value")

	var key_len int64
	//Encode the structure into void pointer.
	encoded_key := Encode(ed, &key_len)
	//Perform the write
	PmdbClientWriteKV(pmdb, rncui, encoded_key, key_len)
}

//Read the value of key on the client
func PmdbClientRead(ed PmdbEncDec, pmdb unsafe.Pointer, rncui string,
					  return_value_len *int64) *C.char {
	//Byte array
	fmt.Println("Client: Read Value for the given Key")

	var key_len int64
	encoded_key := Encode(ed, &key_len)

	var val_len int64
	encoded_val := Encode(ed, &val_len)

	PmdbClientReadKV(pmdb, rncui, encoded_key, key_len, encoded_val, val_len)
	*return_value_len = val_len

	return encoded_val
}

/*
func GoPmdbDecoder(ed PmdbEncDec, buffer_ptr unsafe.Pointer, buf_size int64) {
	data := C.GoBytes(unsafe.Pointer(buffer_ptr), C.int(buf_size))
	byte_arr := bytes.NewBuffer(data)

	decode := gob.NewDecoder(byte_arr)
	for {
		ed.PmdbDecode(decode)
	}
}
*/

/*
 The following goApply and goRead functions are the exported
 functions which is needed for calling the golang function
 pointers from C.
*/

//export goApply
func goApply(app_id *C.struct_raft_net_client_user_id, input_buf unsafe.Pointer,
			input_buf_sz C.size_t, pmdb_handle unsafe.Pointer,
            user_data unsafe.Pointer) {

	//Restore the golang function pointers stored in PmdbCallbacks.
	gcb := gopointer.Restore(user_data).(*PmdbCallbacks)

	//Convert buffer size from c data type size_t to golang int64.
	input_buf_sz_go := CToGoInt64(input_buf_sz)

	//Calling the golang Application's Apply function.
	gcb.ApplyCb(unsafe.Pointer(app_id), input_buf, input_buf_sz_go, pmdb_handle)
}

//export goRead
func goRead(app_id *C.struct_raft_net_client_user_id, request_buf unsafe.Pointer,
            request_bufsz C.size_t, reply_buf unsafe.Pointer, reply_bufsz C.size_t,
            user_data unsafe.Pointer) {

	//Restore the golang function pointers stored in PmdbCallbacks.
	gcb := gopointer.Restore(user_data).(*PmdbCallbacks)

	//Convert buffer size from c data type size_t to golang int64.
	request_bufsz_go := CToGoInt64(request_bufsz)
	reply_bufsz_go := CToGoInt64(reply_bufsz)

	//Calling the golang Application's Read function.
	gcb.ReadCb(unsafe.Pointer(app_id), request_buf, request_bufsz_go, reply_buf, reply_bufsz_go)
}

/**
 * Start the pmdb server.
 * @raft_uuid: Raft UUID.
 * @peer_uuid: Peer UUID.
 * @cf: Column Family
 * @cb: PmdbAPI callback funcs.
 */
func PmdbStartServer(raft_uuid string, peer_uuid string, cf string,
					 cb *PmdbCallbacks) {

	/*
	 * Convert the raft_uuid and peer_uuid go strings into C strings
	 * so that we can pass these to C function.
	 */

	raft_uuid_c := GoToCString(raft_uuid)

	peer_uuid_c := GoToCString(peer_uuid)

	cCallbacks := C.struct_PmdbAPI{}

	//Assign the callback functions for apply and read
	cCallbacks.pmdb_apply = C.pmdb_apply_sm_handler_t(C.applyCgo)
	cCallbacks.pmdb_read = C.pmdb_read_sm_handler_t(C.readCgo)

	/*
	 * Store the column family name into char * array.
	 * Store gostring to byte array.
	 * Don't forget to append the null terminating character.
	 */
	cf_byte_arr := []byte(cf + "\000")

	cf_name := make(charsSlice, len(cf_byte_arr))
	cf_name[0] = (*C.char)(C.CBytes(cf_byte_arr))

	//Convert Byte array to char **
	sH := (*reflect.SliceHeader)(unsafe.Pointer(&cf_name))
	cf_array := (**C.char)(unsafe.Pointer(sH.Data))

	// Create an opaque C pointer for cbs to pass to PmdbStartServer.
	opa_ptr:= gopointer.Save(cb)
	defer gopointer.Unref(opa_ptr)

	// Starting the pmdb server.
	C.PmdbExec(raft_uuid_c, peer_uuid_c, &cCallbacks, cf_array, 1, true, opa_ptr)
}

func PmdbLookupKey(key string, key_len int64, value string,
				   go_cf string) string {


	var goerr string
	var C_value_len C.size_t

	err := GoToCString(goerr)

	cf := GoToCString(go_cf)

	//Convert go string to C char *
	C_key := GoToCString(key)

	C_key_len := GoToCSize_t(key_len)

	//Get the column family handle
	cf_handle := C.PmdbCfHandleLookup(cf)

	ropts := C.rocksdb_readoptions_create()

	C_value := C.rocksdb_get_cf(C.PmdbGetRocksDB(), ropts, cf_handle, C_key,
							  C_key_len, &C_value_len, &err)

	fmt.Println("Lookup for key before updating its value: ", C_key)
	fmt.Println("Value is: ", C_value)

	go_value := CToGoString(C_value)
	fmt.Println("Value is: ", go_value)

	return go_value
}

func PmdbWriteKV(app_id unsafe.Pointer, pmdb_handle unsafe.Pointer, key string,
			   key_len int64, value string, value_len int64, gocolfamily string) {

	//typecast go string to C char *
	cf := GoToCString(gocolfamily)

	go_value := PmdbLookupKey(key, key_len, value, gocolfamily)

	C_key := GoToCString(key)

	C_key_len := GoToCSize_t(key_len)

	C_value := GoToCString(value)

	C_value_len := GoToCSize_t(value_len)

	capp_id := (*C.struct_raft_net_client_user_id)(app_id)

	fmt.Println("Lookup for key before updating its value: ", C_key)

	if go_value != "0" {

		//Convert C string to golang string
		//Append null terminator
		go_result := go_value + "\000"

		fmt.Println("Existing key value", go_result)
	}

	cf_handle := C.PmdbCfHandleLookup(cf)
	//Calling pmdb library function to write Key-Value.
	C.PmdbWriteKV(capp_id, pmdb_handle, C_key, C_key_len, C_value, C_value_len, nil, unsafe.Pointer(cf_handle))

}

func PmdbReadKV(app_id unsafe.Pointer, key string,
			  key_len int64, reply_buf unsafe.Pointer, reply_bufsz int64,
			  gocolfamily string) {

	var value string
	//Convert the golang string to C char*

	go_value := PmdbLookupKey(key, key_len, value, gocolfamily)

	//Get the result
	result := go_value + "\000"
	fmt.Println("Result of the lookup is: ", result)
}

func PmdbStartClient(Graft_uuid string, Gclient_uuid string) unsafe.Pointer {

	raft_uuid := GoToCString(Graft_uuid)

	client_uuid := GoToCString(Gclient_uuid)

	//Start the client.
	Cpmdb := C.PmdbClientStart(raft_uuid, client_uuid)
	return unsafe.Pointer(Cpmdb)
}

func PmdbClientWriteKV(pmdb unsafe.Pointer, rncui string, key *C.char,
					 key_len int64) {

	var obj_stat C.pmdb_obj_stat_t

	crncui_str := GoToCString(rncui)

	c_key_len := GoToCSize_t(key_len)

	var rncui_id C.struct_raft_net_client_user_id

	C.raft_net_client_user_id_parse(crncui_str, &rncui_id, 0)
	var obj_id *C.pmdb_obj_id_t

	obj_id = (*C.pmdb_obj_id_t)(&rncui_id.rncui_key)

	Cpmdb := (C.pmdb_t)(pmdb)

	C.PmdbObjPut(Cpmdb, obj_id, key, c_key_len, &obj_stat)
}

func PmdbClientReadKV(pmdb unsafe.Pointer, rncui string, key *C.char,
		    key_len int64, value *C.char, value_len int64) {
	var obj_stat C.pmdb_obj_stat_t

	crncui_str := GoToCString(rncui)

	c_key_len := GoToCSize_t(key_len)
	c_value_len := GoToCSize_t(value_len)

	var rncui_id C.struct_raft_net_client_user_id

	C.raft_net_client_user_id_parse(crncui_str, &rncui_id, 0)
	var obj_id *C.pmdb_obj_id_t

	obj_id = (*C.pmdb_obj_id_t)(&rncui_id.rncui_key)

	Cpmdb := (C.pmdb_t)(pmdb)
	C.PmdbObjGetX(Cpmdb, obj_id, key, c_key_len, value, c_value_len,
			&obj_stat)
}
