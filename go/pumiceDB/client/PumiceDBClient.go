package PumiceDBClient

import (
	"bytes"
	"errors"
	"fmt"
	PumiceDBCommon "niova/go-pumicedb-lib/common"
	"strconv"
	"syscall"
	"unsafe"

	"github.com/google/uuid"
)

/*
#cgo LDFLAGS: -lniova -lniova_raft_client -lniova_pumice_client
#include <raft/pumice_db_client.h>
#include <raft/pumice_db_net.h>
*/
import "C"

type PmdbReqArgs struct {
	Rncui       string
	ReqED       interface{}
	ResponseED  interface{}
	ReqByteArr  []byte
	ReqType     int
	Response    *[]byte
	ReplySize   *int64
	GetResponse int
	ZeroCopyObj *RDZeroCopyObj
}

type PmdbClientObj struct {
	initialized bool
	pmdb        C.pmdb_t
	raftUuid    string
	myUuid      string
	AppUUID     string
	WriteSeqNo  uint64
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

//Get PumiceRequest in common format
func getPmdbReq(reqArgs *PmdbReqArgs) (unsafe.Pointer, error) {
	// get bytes for requestResponse.Request and
	// convert PumiceDBCommon.PumiceRequest
	var pmdbRequest PumiceDBCommon.PumiceRequest

	pmdbRequest.ReqType = reqArgs.ReqType
	pmdbRequest.ReqPayload = reqArgs.ReqByteArr

	var reqLen int64
	encodedReqPtr, err := PumiceDBCommon.Encode(pmdbRequest, &reqLen)
	if err != nil {
		return nil, err
	}

	return encodedReqPtr, err

}

//Write KV from client.
func (obj *PmdbClientObj) Write(reqArgs *PmdbReqArgs) (unsafe.Pointer, error) {

	var key_len int64

	// Encode the application structure into void pointer so it can be
	// type casted to C char *
	ed_key, err := PumiceDBCommon.Encode(reqArgs.ReqED, &key_len)
	if err != nil {
		return nil, err
	}

	//Typecast the encoded key to char*
	encoded_key := (*C.char)(ed_key)
	getResponse_c := (C.int)(reqArgs.GetResponse)
	//Perform the write
	return obj.writeKV(reqArgs.Rncui, encoded_key, key_len, getResponse_c,
		reqArgs.ReplySize)
}

//WriteEncoded
/*
WriteEncoded allows client to pass the encoded KV struct for writing
*/
func (obj *PmdbClientObj) WriteEncoded(reqArgs *PmdbReqArgs) (unsafe.Pointer,
	error) {
	requestLen := int64(len(reqArgs.ReqByteArr))
	//Convert it to unsafe pointer (void * for C function)
	//encodedData := unsafe.Pointer(&reqArgs.ReqByteArr[0])
	encodedData, err := getPmdbReq(reqArgs)
	if err != nil {
		return nil, fmt.Errorf("PumiceDBClient - Error while converting AppReq to PmdbReq : ", err)
	}
	encodedRequest := (*C.char)(encodedData)
	getResponse_c := (C.int)(reqArgs.GetResponse)
	return obj.writeKV(reqArgs.Rncui, encodedRequest, requestLen,
		getResponse_c,
		reqArgs.ReplySize)

}

func (obj *PmdbClientObj) WriteEncodedAndGetResponse(reqArgs *PmdbReqArgs) error {
	var reply_size int64
	var wr_err error
	var reply_buff unsafe.Pointer

	requestLen := int64(len(reqArgs.ReqByteArr))
	//Convert to unsafe pointer (void * for C function)
	//encodedData := unsafe.Pointer(&reqArgs.ReqByteArr[0])
	encodedData, err := getPmdbReq(reqArgs)
	if err != nil {
		return fmt.Errorf("PumiceDBClient - Error while converting AppReq to PmdbReq : ", err)
	}
	encodedRequest := (*C.char)(encodedData)
	getResponse_c := (C.int)(reqArgs.GetResponse)

	reply_buff, wr_err = obj.writeKV(reqArgs.Rncui, encodedRequest,
		requestLen, getResponse_c, &reply_size)
	if wr_err != nil {
		return wr_err
	}

	if reply_buff != nil {
		bytes_data := C.GoBytes(unsafe.Pointer(reply_buff), C.int(reply_size))
		buffer := bytes.NewBuffer(bytes_data)
		*reqArgs.Response = buffer.Bytes()
	}
	// Free the buffer allocated by the C library
	C.free(reply_buff)
	return nil
}

//Read the value of key on the client
func (obj *PmdbClientObj) Read(reqArgs *PmdbReqArgs) error {

	var key_len int64
	var reply_size int64
	var rd_err error
	var reply_buff unsafe.Pointer

	//Encode the data passed by application.
	ed_key, err := PumiceDBCommon.Encode(reqArgs.ReqED, &key_len)
	if err != nil {
		return err
	}

	//Typecast the encoded key to char*
	encoded_key := (*C.char)(ed_key)

	if len(reqArgs.Rncui) == 0 {
		reply_buff, rd_err = obj.readKVAny(encoded_key,
			key_len, &reply_size)
	} else {
		reply_buff, rd_err = obj.readKV(reqArgs.Rncui, encoded_key,
			key_len, &reply_size)
	}

	if rd_err != nil {
		return rd_err
	}

	if reply_buff != nil {
		err = PumiceDBCommon.Decode(unsafe.Pointer(reply_buff),
			reqArgs.ResponseED,
			reply_size)
	}
	//Free the buffer allocated by C library.
	C.free(reply_buff)
	return err
}

//ReadEncoded
/*
ReadEncoded allows client to pass the encoded KV struct for reading
*/
func (obj *PmdbClientObj) ReadEncoded(reqArgs *PmdbReqArgs) error {
	var reply_size int64
	var rd_err error
	var reply_buff unsafe.Pointer

	requestLen := int64(len(reqArgs.ReqByteArr))
	//Typecast the encoded key to char*
	//encodedData := unsafe.Pointer(&reqArgs.ReqByteArr[0])
	encodedData, err := getPmdbReq(reqArgs)
	if err != nil {
		return fmt.Errorf("PumiceDBClient - Error while converting AppReq to PmdbReq : ", err)
	}
	encoded_key := (*C.char)(encodedData)

	if len(reqArgs.Rncui) == 0 {
		reply_buff, rd_err = obj.readKVAny(encoded_key,
			requestLen, &reply_size)
	} else {
		reply_buff, rd_err = obj.readKV(reqArgs.Rncui, encoded_key,
			requestLen, &reply_size)
	}

	if rd_err != nil {
		return rd_err
	}

	if reply_buff != nil {
		bytes_data := C.GoBytes(unsafe.Pointer(reply_buff), C.int(reply_size))
		buffer := bytes.NewBuffer(bytes_data)
		*reqArgs.Response = buffer.Bytes()
	}
	//Free the buffer allocated by C library.
	C.free(reply_buff)
	return nil
}

//Read the value of key on the client the application passed buffer
func (obj *PmdbClientObj) ReadZeroCopy(reqArgs *PmdbReqArgs) error {

	var key_len int64
	//Encode the input buffer passed by client.
	ed_key, err := PumiceDBCommon.Encode(reqArgs.ReqED, &key_len)
	if err != nil {
		return err
	}

	//Typecast the encoded key to char*
	encoded_key := (*C.char)(ed_key)

	//Read the value of the key in application buffer
	return obj.readKVZeroCopy(reqArgs.Rncui, encoded_key,
		key_len, reqArgs.ZeroCopyObj)

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

// Call the pmdb C library function to write the application data.
// If application expects response on write operation,
// get_response should be 1
func (obj *PmdbClientObj) writeKV(rncui string, key *C.char,
	key_len int64, get_response C.int,
	reply_size *int64) (unsafe.Pointer, error) {

	var obj_stat C.pmdb_obj_stat_t

	crncui_str := GoToCString(rncui)
	defer FreeCMem(crncui_str)

	c_key_len := GoToCSize_t(key_len)

	var rncui_id C.struct_raft_net_client_user_id

	C.raft_net_client_user_id_parse(crncui_str, &rncui_id, 0)
	var obj_id *C.pmdb_obj_id_t

	obj_id = (*C.pmdb_obj_id_t)(&rncui_id.rncui_key)

	rc := C.PmdbObjPut(obj.pmdb, obj_id, key, c_key_len, get_response,
		&obj_stat)

	if rc != 0 {
		return nil, fmt.Errorf("PmdbObjPut(): %d", rc)
	}

	get_response_go := int(get_response)
	if get_response_go == 1 {
		reply_buf := obj_stat.reply_buffer
		*reply_size = int64(obj_stat.reply_size)
		return reply_buf, nil
	}

	return nil, nil
}

//Call the pmdb C library function to read the value for the key.
func (obj *PmdbClientObj) readKV(rncui string, key *C.char,
	key_len int64,
	reply_size *int64) (unsafe.Pointer, error) {

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

	if reply_buff == nil {
		*reply_size = 0
		err := errors.New("Key not found")
		return nil, err
	}

	*reply_size = int64(actual_value_size)

	return reply_buff, nil
}

//Call the pmdb C library function to read the value for the key.
func (obj *PmdbClientObj) readKVAny(key *C.char,
	key_len int64,
	reply_size *int64) (unsafe.Pointer, error) {

	var actual_value_size C.size_t
	c_key_len := GoToCSize_t(key_len)

	reply_buff := C.PmdbObjGetAny(obj.pmdb, key, c_key_len,
		&actual_value_size)

	if reply_buff == nil {
		*reply_size = 0
		err := errors.New("Key not found")
		return nil, err
	}

	*reply_size = int64(actual_value_size)

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

	C.pmdb_request_options_init(&pmdb_req_opt, 1, 0, 0, &stat, nil, nil,
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
