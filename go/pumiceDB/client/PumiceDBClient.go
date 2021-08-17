package PumiceDBClient

import (
	"errors"
	"fmt"
	"strconv"
	"syscall"
	"unsafe"

	"github.com/google/uuid"

	"niova/go-pumicedb-lib/common"
)

/*
#cgo LDFLAGS: -lniova -lniova_raft_client -lniova_pumice_client
#include <raft/pumice_db_client.h>
#include <raft/pumice_db_net.h>
extern void asyncCbCgo(void *arg, ssize_t rrc);
*/
import "C"

import gopointer "github.com/mattn/go-pointer"

type PmdbClientAsyncCb interface {
	AsyncCb(args unsafe.Pointer, rrc int64, asyncChan chan int64)
}

//Async request callback function and its argument.
type PmdbAsyncReq struct {
	PmdbAsyncCb PmdbClientAsyncCb
	CbArgs unsafe.Pointer
	CbChan chan int64
}

type PmdbClientObj struct {
	initialized bool
	pmdb        C.pmdb_t
	raftUuid    string
	myUuid      string
	asyncData	PmdbAsyncReq
}

type RDZeroCopyObj struct {
	buffer     unsafe.Pointer
	buffer_len int64
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

func (obj *PmdbClientObj)AsyncCb(args unsafe.Pointer, rrc int64, asyncChan chan int64) {

	//Pass the return code to waiting RW thread using channel.
	asyncChan <- rrc
}

//export goAsyncCb
func goAsyncCb(user_data unsafe.Pointer, rrc C.ssize_t) {
	//Restore the golang function pointers stored in PmdbCallbacks.
	gcb := gopointer.Restore(user_data).(*PmdbAsyncReq)

	//Convert buffer size from c data type size_t to golang int64.
	rrc_go := int64(rrc)

	//Calling the golang Application's Async callback function.
	gcb.PmdbAsyncCb.AsyncCb(gcb.CbArgs, rrc_go, gcb.CbChan)
}

//Write KV from client.
func (obj *PmdbClientObj) Write(ed interface{},
	rncui string, asyncWrites bool) error {

	var key_len int64

	// Encode the application structure into void pointer so it can be
	// type casted to C char *
	ed_key, err := PumiceDBCommon.Encode(ed, &key_len)
	if err != nil {
		return err
	}

	//Typecast the encoded key to char*
	encoded_key := (*C.char)(ed_key)

	//Perform the write
	return obj.writeKV(rncui, encoded_key, key_len, asyncWrites)
}

//Read the value of key on the client
func (obj *PmdbClientObj) Read(input_ed interface{},
	rncui string, output_ed interface{}, asyncRead bool) error {

	var key_len int64
	var reply_size int64

	//Encode the data passed by application.
	ed_key, err := PumiceDBCommon.Encode(input_ed, &key_len)
	if err != nil {
		return err
	}

	//Typecast the encoded key to char*
	encoded_key := (*C.char)(ed_key)

	reply_buff, rd_err := obj.readKV(rncui, encoded_key,
		key_len, &reply_size, asyncRead)

	if rd_err != nil {
		return rd_err
	}

	if reply_buff != nil {
		err = PumiceDBCommon.Decode(unsafe.Pointer(reply_buff), output_ed,
			reply_size)
	}
	//Free the buffer allocated by C library.
	C.free(reply_buff)
	return err
}

//Read the value of key on the client the application passed buffer
func (obj *PmdbClientObj) ReadZeroCopy(input_ed interface{},
	rncui string,
	zeroCopyObj *RDZeroCopyObj) error {

	var key_len int64
	//Encode the input buffer passed by client.
	ed_key, err := PumiceDBCommon.Encode(input_ed, &key_len)
	if err != nil {
		return err
	}

	//Typecast the encoded key to char*
	encoded_key := (*C.char)(ed_key)

	//Read the value of the key in application buffer
	return obj.readKVZeroCopy(rncui, encoded_key,
		key_len, zeroCopyObj)

}

//Get the Leader UUID.
func (pmdb_client *PmdbClientObj) PmdbGetLeader() (uuid.UUID, error) {

	var leader_info C.raft_client_leader_info_t
	Cpmdb := (C.pmdb_t)(pmdb_client.pmdb)

	rc := C.PmdbGetLeaderInfo(Cpmdb, &leader_info)
	if rc != 0 {
		return uuid.Nil, fmt.Errorf("Failed to get leader info (%d)", rc)
	}

	//C uuid to Go bytes
	return uuid.FromBytes(C.GoBytes(unsafe.Pointer(&leader_info.rcli_leader_uuid),
		C.int(unsafe.Sizeof(leader_info.rcli_leader_uuid))))

}

//Call the pmdb C library function to write the application data.
func (obj *PmdbClientObj) writeKV(rncui string, key *C.char,
	key_len int64, asyncWrites bool) error {

	var obj_stat C.pmdb_obj_stat_t
	var rc C.int

	crncui_str := GoToCString(rncui)
	defer FreeCMem(crncui_str)

	c_key_len := GoToCSize_t(key_len)

	var rncui_id C.struct_raft_net_client_user_id

	C.raft_net_client_user_id_parse(crncui_str, &rncui_id, 0)
	var obj_id *C.pmdb_obj_id_t

	obj_id = (*C.pmdb_obj_id_t)(&rncui_id.rncui_key)

	if !asyncWrites {
		//Synchronous write request
		rc = C.PmdbObjPut(obj.pmdb, obj_id, key, c_key_len, &obj_stat)
	} else {
		// Asynchronous write request

		/* Create the channel which can be used to communicate between
		 * current process and callback which will be executed on write
		 * completion
		 */
		asyncChan := make(chan int64)
		asyncReq := &PmdbAsyncReq{
			CbArgs: unsafe.Pointer(key),
			PmdbAsyncCb: obj,
			CbChan: asyncChan,
		}

		//Async write request
		var pmdbReq C.pmdb_request_opts_t
		// Create an opaque C pointer for cbs to pass to PmdbObjPutX
		opa_ptr := gopointer.Save(asyncReq)
		defer gopointer.Unref(opa_ptr)

		C.pmdb_request_options_init(&pmdbReq, 1, 1, &obj_stat,
			(*[0]byte)(C.asyncCbCgo), opa_ptr, nil, 0, 0)

		//execute the PmdbObjPutX in goroutine.
		go C.PmdbObjPutX(obj.pmdb, obj_id, key, c_key_len, &pmdbReq)

		// Wait for the write to complete..
		rc = C.int(<-asyncChan)
	}

	if rc != 0 {
		var errno syscall.Errno
		return fmt.Errorf("PmdbObjPut/X() failed: %d", errno)
	}

	return nil
}

//Call the pmdb C library function to read the value for the key.
func (obj *PmdbClientObj) readKV(rncui string, key *C.char,
	key_len int64,
	reply_size *int64, asyncRead bool) (unsafe.Pointer, error) {

	crncui_str := GoToCString(rncui)
	defer FreeCMem(crncui_str)

	c_key_len := GoToCSize_t(key_len)

	var rncui_id C.struct_raft_net_client_user_id

	C.raft_net_client_user_id_parse(crncui_str, &rncui_id, 0)
	var obj_id *C.pmdb_obj_id_t

	obj_id = (*C.pmdb_obj_id_t)(&rncui_id.rncui_key)

	var actual_value_size C.size_t
	var reply_buff unsafe.Pointer

	if !asyncRead {
		// Synchronous read request.
		reply_buff = C.PmdbObjGet(obj.pmdb, obj_id, key, c_key_len,
			&actual_value_size)
		if reply_buff == nil {
			*reply_size = 0
			err := errors.New("Key not found")
			return nil, err
		}

		*reply_size = int64(actual_value_size)

	} else {

		// Async read request.
		//Create channel to communicate between current process and callback.
		asyncChan := make(chan int64)

		asyncReq := &PmdbAsyncReq{
			CbArgs: unsafe.Pointer(key),
			PmdbAsyncCb: obj,
			CbChan: asyncChan,
		}

		//Async write request
		var obj_stat C.pmdb_obj_stat_t
		var pmdb_req_opt C.pmdb_request_opts_t
		// Create an opaque C pointer for cbs to pass to PmdbObjPutX
		opa_ptr := gopointer.Save(asyncReq)
		defer gopointer.Unref(opa_ptr)

		C.pmdb_request_options_init(&pmdb_req_opt, 1, 1, &obj_stat,
			(*[0]byte)(C.asyncCbCgo), opa_ptr, nil, 0, 0)

		// execute PmdbObjGetX in goroutine.
		go C.PmdbObjGetX(obj.pmdb, obj_id, key, c_key_len, &pmdb_req_opt)

		// Wait for the operation to complete
		rc := C.int(<-asyncChan)

		if rc != 0 {
			var errno syscall.Errno
			return nil, fmt.Errorf("PmdbObjGetX(): %d", errno)
		}
		// Get the reply buffer and reply size from obj_stat
		reply_buff = unsafe.Pointer(obj_stat.reply_buffer)
		*reply_size = int64(obj_stat.reply_size)
	}
	return reply_buff, nil
}

//Allocate memory in C heap
func (obj *RDZeroCopyObj) AllocateCMem(size int64) unsafe.Pointer {
	return C.malloc(C.size_t(size))
}

//Relase the C memory allocated for reading the value
func (obj *RDZeroCopyObj) ReleaseCMem() {
	C.free(obj.buffer)
}

/*
 * Note the data is not decoded in this method. Application should
 * take care of decoding the buffer data.
 */
func (obj *PmdbClientObj) readKVZeroCopy(rncui string, key *C.char,
	key_len int64,
	zeroCopyObj *RDZeroCopyObj) error {

	crncui_str := GoToCString(rncui)
	defer FreeCMem(crncui_str)

	c_key_len := GoToCSize_t(key_len)

	var rncui_id C.struct_raft_net_client_user_id

	C.raft_net_client_user_id_parse(crncui_str, &rncui_id, 0)
	var obj_id *C.pmdb_obj_id_t

	obj_id = (*C.pmdb_obj_id_t)(&rncui_id.rncui_key)

	var stat C.pmdb_obj_stat_t
	var pmdb_req_opt C.pmdb_request_opts_t

	C.pmdb_request_options_init(&pmdb_req_opt, 1, 0, &stat, nil, nil,
		zeroCopyObj.buffer, C.size_t(zeroCopyObj.buffer_len), 0)

	rc := C.PmdbObjGetX(obj.pmdb, obj_id, key, c_key_len,
		&pmdb_req_opt)

	if rc != 0 {
		return fmt.Errorf("PmdbObjGetX(): return code: %d", rc)
	}

	return nil
}

// Return the decode / encode size of the provided object
func (obj *PmdbClientObj) GetSize(ed interface{}) int64 {
	return PumiceDBCommon.GetStructSize(ed)
}

// Decode in the input buffer into the output object
// XXX note this function *should* return an error
func (obj *PmdbClientObj) Decode(input unsafe.Pointer, output interface{},
	len int64) error {
	return PumiceDBCommon.Decode(input, output, len)
}

// Stop the Pmdb client instance
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

//Start the Pmdb client instance
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
