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

type GoApplyCallback func(*C.struct_raft_net_client_user_id, unsafe.Pointer, int64,
                          unsafe.Pointer)
type GoReadCallback func(*C.struct_raft_net_client_user_id, unsafe.Pointer, int64,
                         unsafe.Pointer, int64)

//Callback function for pmdb apply and read
type GoPmdbCallbacks struct {
	ApplyCb GoApplyCallback
	ReadCb GoReadCallback
}

var Cpmdb C.pmdb_t //pmdb pointer

type charsSlice []*C.char

//Generic encoder/decoder interface
type goPmdbEncDec interface {
	PmdbEncode(enc *gob.Encoder)
}
type cfSlice []*C.char

func GoEncode(ed goPmdbEncDec, data_len *int64) *C.char {
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
func GoPmdbClientWrite(ed goPmdbEncDec, rncui string) {

	fmt.Println("Client: Write Key-Value")

	var key_len int64
	//Encode the structure into void pointer.
	encoded_key := GoEncode(ed, &key_len)
	//Perform the write
	GoClientWriteKV(rncui, encoded_key, key_len)
}

//Read the value of key on the client
func GoPmdbClientRead(ed goPmdbEncDec, rncui string, return_value_len *int64) *C.char {
	//Byte array
	fmt.Println("Client: Read Value for the given Key")

	var key_len int64
	encoded_key := GoEncode(ed, &key_len)

	var val_len int64
	encoded_val := GoEncode(ed, &val_len)

	GoClientReadKV(rncui, encoded_key, key_len, encoded_val, val_len)
	*return_value_len = val_len

	return encoded_val
}

/*
func GoPmdbDecoder(ed goPmdbEncDec, buffer_ptr unsafe.Pointer, buf_size int64) {
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

	//Restore the golang function pointers stored in GoPmdbCallbacks.
	gcb := gopointer.Restore(user_data).(*GoPmdbCallbacks)

	//Convert buffer size from c data type size_t to golang int64.
	input_buf_sz_go := int64(input_buf_sz)

	//Calling the golang Application's Apply function.
	gcb.ApplyCb(app_id, input_buf, input_buf_sz_go, pmdb_handle)
}

//export goRead
func goRead(app_id *C.struct_raft_net_client_user_id, request_buf unsafe.Pointer,
            request_bufsz C.size_t, reply_buf unsafe.Pointer, reply_bufsz C.size_t,
            user_data unsafe.Pointer) {

	//Restore the golang function pointers stored in GoPmdbCallbacks.
	gcb := gopointer.Restore(user_data).(*GoPmdbCallbacks)

	//Convert buffer size from c data type size_t to golang int64.
	request_bufsz_go := int64(request_bufsz)
	reply_bufsz_go := int64(reply_bufsz)

	//Calling the golang Application's Read function.
	gcb.ReadCb(app_id, request_buf, request_bufsz_go, reply_buf, reply_bufsz_go)
}


func GoStartServer(raft_uuid string, peer_uuid string, cf string, ptr unsafe.Pointer) {

	//Convert the raft_uuid and peer_uuid go strings into C strings
	//so that we can pass these to C function.
	raft_uuid_c := C.CString(raft_uuid)
	defer C.free(unsafe.Pointer(raft_uuid_c))

	peer_uuid_c := C.CString(peer_uuid)
	defer C.free(unsafe.Pointer(peer_uuid_c))

	cCallbacks := C.struct_PmdbAPI{}

	//Assign the callback functions for apply and read
	cCallbacks.pmdb_apply = C.pmdb_apply_sm_handler_t(C.applyCgo)
	cCallbacks.pmdb_read = C.pmdb_read_sm_handler_t(C.readCgo)

	//Store the column family name into char * array.
	//Store gostring to byte array
	cf_byte_arr := []byte(cf)
	cf_name := make(charsSlice, len(cf_byte_arr))
	cf_name[0] = (*C.char)(C.CBytes(cf_byte_arr))

	//Convert Byte array to char **
	sH := (*reflect.SliceHeader)(unsafe.Pointer(&cf_name))
	cf_array := (**C.char)(unsafe.Pointer(sH.Data))

	// Starting the pmdb server.
	C.PmdbExecGo(raft_uuid_c, peer_uuid_c, &cCallbacks, cf_array, 1, true, ptr)
}

func GoWriteKV(app_id *C.struct_raft_net_client_user_id, pmdb_handle unsafe.Pointer, key string,
			   key_len int64, value string, value_len int64, gocolfamily string) {

	cf := C.CString(gocolfamily)
	defer C.free(unsafe.Pointer(cf))

	//Convert go string to C char *
	C_key := C.CString(key)
	defer C.free(unsafe.Pointer(C_key))

	C_key_len := C.size_t(key_len)

	C_value := C.CString(value)
	defer C.free(unsafe.Pointer(C_value))

	C_value_len := C.size_t(value_len)

	//Calling pmdb library function to write Key-Value.
	C.PmdbWriteKVGo(app_id, pmdb_handle, C_key, C_key_len, C_value, C_value_len, nil, cf)

}

func GoReadKV(app_id *C.struct_raft_net_client_user_id, key string,
			  key_len int64, reply_buf unsafe.Pointer, reply_bufsz int64,
			  gocolfamily string) {

	//Convert the golang string to C char*
	cf_name := C.CString(gocolfamily)
	defer C.free(unsafe.Pointer(cf_name))

	C_key := C.CString(key)
	defer C.free(unsafe.Pointer(C_key))

	C_key_len := C.size_t(key_len)

	var C_value *C.char
	rc := C.Pmdb_test_app_lookup(app_id, C_key, C_key_len, C_value, cf_name)
	fmt.Println("Return value of lookup: ", rc)
}

func GoStartClient(Graft_uuid string, Gclient_uuid string) {

	raft_uuid := C.CString(Graft_uuid)
	defer C.free(unsafe.Pointer(raft_uuid))

	client_uuid := C.CString(Gclient_uuid)
	defer C.free(unsafe.Pointer(client_uuid))

	//Start the client.
	Cpmdb  = C.PmdbClientStart(raft_uuid, client_uuid)
}

func GoClientWriteKV(rncui string, key *C.char,
					 key_len int64) {

	var obj_stat C.pmdb_obj_stat_t
	crncui_str := C.CString(rncui)
	defer C.free(unsafe.Pointer(crncui_str))

	c_key_len := C.size_t(key_len)

	var rncui_id C.struct_raft_net_client_user_id

	C.raft_net_client_user_id_parse(crncui_str, &rncui_id, 0)
	var obj_id *C.pmdb_obj_id_t

	obj_id = (*C.pmdb_obj_id_t)(&rncui_id.rncui_key)

	C.PmdbObjPut(Cpmdb, obj_id, key, c_key_len, &obj_stat)
}

func GoClientReadKV(rncui string, key *C.char,
		    key_len int64, value *C.char, value_len int64) {
	var obj_stat C.pmdb_obj_stat_t

	crncui_str := C.CString(rncui)
	defer C.free(unsafe.Pointer(crncui_str))

	c_key_len := C.size_t(key_len)
	c_value_len := C.size_t(value_len)

	var rncui_id C.struct_raft_net_client_user_id

	C.raft_net_client_user_id_parse(crncui_str, &rncui_id, 0)
	var obj_id *C.pmdb_obj_id_t

	obj_id = (*C.pmdb_obj_id_t)(&rncui_id.rncui_key)
	C.PmdbObjGetX(Cpmdb, obj_id, key, c_key_len, value, c_value_len,
			&obj_stat)
}
