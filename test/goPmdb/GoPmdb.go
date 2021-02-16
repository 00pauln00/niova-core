package GoPmdb
import (
	"fmt"
	"unsafe"
)

/*
#cgo pkg-config: niova --define-variable=prefix=/home/manisha/binaries
#include <raft/pumice_db.h>
#include <rocksdb/c.h>
#include <raft/raft_net.h>
extern void applyCgo(const void *, const void *,
                     size_t, void *, void *);
extern void readCgo(const void *, const void *,
                    size_t, void *, size_t, void *);
*/
import "C" //There should be no empty line between above c declarations and import "c"

import gopointer "github.com/mattn/go-pointer"

type GoApplyCallback func(unsafe.Pointer, unsafe.Pointer, int64,
                          unsafe.Pointer)
type GoReadCallback func(unsafe.Pointer, unsafe.Pointer, int64,
                         unsafe.Pointer, int64)

type GoPmdbCallbacks struct {
	ApplyCb GoApplyCallback
	ReadCb GoReadCallback
}

//export goApply
func goApply(app_id unsafe.Pointer, input_buf unsafe.Pointer,
			input_buf_sz C.size_t, pmdb_handle unsafe.Pointer,
            user_data unsafe.Pointer) {
	fmt.Println("Inside Go apply and now calling that function")
	gcb := gopointer.Restore(user_data).(*GoPmdbCallbacks)
	input_buf_sz_go := int64(input_buf_sz)
	gcb.ApplyCb(app_id, input_buf, input_buf_sz_go, pmdb_handle)
}

//export goRead
func goRead(app_id unsafe.Pointer, request_buf unsafe.Pointer,
            request_bufsz C.size_t, reply_buf unsafe.Pointer, reply_bufsz C.size_t,
            user_data unsafe.Pointer) {
	fmt.Println("Inside Go Read and now calling that function")
	gcb := gopointer.Restore(user_data).(*GoPmdbCallbacks)
	request_bufsz_go := int64(request_bufsz)
	reply_bufsz_go := int64(reply_bufsz)
	gcb.ReadCb(app_id, request_buf, request_bufsz_go, reply_buf, reply_bufsz_go)
}

func GoStartServer(raft_uuid string, peer_uuid string, ptr unsafe.Pointer) {
	//Convert the raft_uuid and peer_uuid go strings into C strings
	//so that we can pass these to C function.

	raft_uuid_c := C.CString(raft_uuid)
	defer C.free(unsafe.Pointer(raft_uuid_c))

	peer_uuid_c := C.CString(peer_uuid)
	defer C.free(unsafe.Pointer(peer_uuid_c))

	cCallbacks := C.struct_PmdbAPI{}

	cCallbacks.pmdb_apply = C.pmdb_apply_sm_handler_t(C.applyCgo)
	cCallbacks.pmdb_read = C.pmdb_read_sm_handler_t(C.readCgo)

	fmt.Println("Calling C function")
	C.PmdbExecGo(raft_uuid_c, peer_uuid_c, &cCallbacks, 1, true, ptr)
}

func GoWriteKV(app_id unsafe.Pointer, pmdb_handle unsafe.Pointer, key string,
			   key_len int64, value string, value_len int64, gocolfamily string) {

	cf := C.CString(gocolfamily)
	defer C.free(unsafe.Pointer(cf))

	//Convert go string to C char *
	C_key := C.CString(key)
	defer C.free(unsafe.Pointer(C_key))

	go_key_len := len(key)
	C_key_len := C.size_t(go_key_len)

	C_value := C.CString(value)
	defer C.free(unsafe.Pointer(C_value))

	C_value_len := C.size_t(value_len)

	//Calling pmdb library function.
	C.PmdbWriteKVGo(app_id, pmdb_handle, C_key, C_key_len, C_value, C_value_len, nil, cf)

}

func GoReadKV(app_id unsafe.Pointer, key string,
			  key_len int64, reply_buf unsafe.Pointer, reply_bufsz int64,
			  gocolfamily string) {
	cf_name := C.CString(gocolfamily)
	defer C.free(unsafe.Pointer(cf_name))

	C_key := C.CString(key)
	defer C.free(unsafe.Pointer(C_key))

	C_key_len := C.size_t(key_len)

	var C_value *C.char
	rc := C.Pmdb_test_app_lookup(app_id, C_key, C_key_len, C_value, cf_name)
	fmt.Println("Return value is", rc)
}
