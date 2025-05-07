package PumiceDBServer

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	gopointer "github.com/mattn/go-pointer"
	uuid "github.com/satori/go.uuid"
	"io"
	"math"
	PumiceDBCommon "niova/go-pumicedb-lib/common"
	"reflect"
	"strconv"
	"strings"
	"unsafe"

	log "github.com/sirupsen/logrus"
)

/*
#cgo LDFLAGS: -lniova -lniova_raft -lniova_pumice -lrocksdb
#include <raft/pumice_db.h>
#include <rocksdb/c.h>
#include <raft/raft_net.h>
#include <raft/pumice_db_client.h>
extern ssize_t writePrepCgo(struct pumicedb_cb_cargs *args, int *);
extern ssize_t applyCgo(struct pumicedb_cb_cargs *args, void *);
extern ssize_t readCgo(struct pumicedb_cb_cargs *args);
extern void initCgo(struct pumicedb_cb_cargs *args);
*/
import "C"

// The encoding overhead for a single key-val entry is 2 bytes
var encodingOverhead int = 2

type PmdbCbArgs struct {
	UserID      unsafe.Pointer
	ReqBuf      unsafe.Pointer
	ReqSize     int64
	ReplyBuf    unsafe.Pointer
	ReplySize   int64
	InitState   int
	Payload     []byte
	ContinueWr  unsafe.Pointer
	PmdbHandler unsafe.Pointer
	UserData    unsafe.Pointer
}

type PmdbServerAPI interface {
	WritePrep(goCbArgs *PmdbCbArgs) int64
	Apply(goCbArgs *PmdbCbArgs) int64
	Read(goCbArgs *PmdbCbArgs) int64
	Init(goCbArgs *PmdbCbArgs)
}

type LeaseServerAPI interface {
	WritePrep(goCbArgs *PmdbCbArgs) int64
	Apply(goCbArgs *PmdbCbArgs) int64
	Read(goCbArgs *PmdbCbArgs) int64
	Init(goCbArgs *PmdbCbArgs)
}

type PmdbServerObject struct {
	PmdbAPI        PmdbServerAPI
	LeaseAPI       LeaseServerAPI
	RaftUuid       string
	PeerUuid       string
	SyncWrites     bool
	CoalescedWrite bool
	LeaseEnabled   bool
	ColumnFamilies []string
}

type PmdbLeaderTS struct {
	Term int64
	Time int64
}

const (
	INIT_TYPE_NONE                int = 0
	INIT_BOOTUP_STATE                 = 1
	INIT_BECOMING_LEADER_STATE        = 2
	INIT_BECOMING_CANDIDATE_STATE     = 3
	INIT_SHUTDOWN_STATE               = 4
	INIT_TYPE_ANY                     = 5
)

type charsSlice []*C.char

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

/* Get length of the Go string */
func GoStringLen(str string) int {
	return len(str)
}

/* Type cast Go int64 to C size_t */
func GoToCSize_t(glen int64) C.size_t {
	return C.size_t(glen)
}

/* Typecast C size_t to Go int64 */
func CToGoInt64(cvalue C.size_t) int64 {
	return int64(cvalue)
}

/* Typecast C uint64_t to Go uint64*/
func CToGoUint64(cvalue C.ulong) uint64 {
	return uint64(cvalue)
}

/* Type cast C char * to Go string */
func CToGoBytes(C_value *C.char, C_value_len C.int) []byte {
	return C.GoBytes(unsafe.Pointer(C_value), C_value_len)
}

func pmdbCbArgsInit(cargs *C.struct_pumicedb_cb_cargs,
	goCbArgs *PmdbCbArgs) int {
	goCbArgs.UserID = unsafe.Pointer(cargs.pcb_userid)
	ReqBuf := unsafe.Pointer(cargs.pcb_req_buf)
	ReqSize := CToGoInt64(cargs.pcb_req_bufsz)
	goCbArgs.ReplyBuf = unsafe.Pointer(cargs.pcb_reply_buf)
	goCbArgs.ReplySize = CToGoInt64(cargs.pcb_reply_bufsz)
	goCbArgs.InitState = int(cargs.pcb_init)
	goCbArgs.ContinueWr = unsafe.Pointer(cargs.pcb_continue_wr)
	goCbArgs.PmdbHandler = unsafe.Pointer(cargs.pcb_pmdb_handler)
	goCbArgs.UserData = unsafe.Pointer(cargs.pcb_user_data)
	//Decode Pumice level request
	request := &PumiceDBCommon.PumiceRequest{}
	err := Decode(ReqBuf, request, ReqSize)
	if err != nil {
		log.Error(err)
		return -1
	}
	goCbArgs.Payload = request.ReqPayload
	return request.ReqType
}

/*
 The following goWritePrep, goApply and goRead functions are the exported
 functions which is needed for calling the golang function
 pointers from C.
*/

//export goWritePrep
func goWritePrep(args *C.struct_pumicedb_cb_cargs) int64 {

	var wrPrepArgs PmdbCbArgs
	reqType := pmdbCbArgsInit(args, &wrPrepArgs)

	//Restore the golang function pointers stored in PmdbCallbacks.
	gcb := gopointer.Restore(wrPrepArgs.UserData).(*PmdbServerObject)

	var ret int64
	if reqType == PumiceDBCommon.APP_REQ {
		//Calling the golang Application's WritePrep function.
		ret = gcb.PmdbAPI.WritePrep(&wrPrepArgs)
	} else if reqType == PumiceDBCommon.LEASE_REQ {
		//Calling leaseAPP WritePrep
		ret = gcb.LeaseAPI.WritePrep(&wrPrepArgs)
	} else {
		return -1
	}
	return ret
}

//export goApply
func goApply(args *C.struct_pumicedb_cb_cargs,
	pmdb_handle unsafe.Pointer) int64 {

	var applyArgs PmdbCbArgs
	reqType := pmdbCbArgsInit(args, &applyArgs)

	//Restore the golang function pointers stored in PmdbCallbacks.
	gcb := gopointer.Restore(applyArgs.UserData).(*PmdbServerObject)

	var ret int64
	if reqType == PumiceDBCommon.APP_REQ {
		//Calling the golang Application's Apply function.
		ret = gcb.PmdbAPI.Apply(&applyArgs)
	} else if reqType == PumiceDBCommon.LEASE_REQ {
		//Calling leaseAPP Apply
		ret = gcb.LeaseAPI.Apply(&applyArgs)
	}
	return ret
}

//export goRead
func goRead(args *C.struct_pumicedb_cb_cargs) int64 {

	var readArgs PmdbCbArgs
	reqType := pmdbCbArgsInit(args, &readArgs)

	//Restore the golang function pointers stored in PmdbCallbacks.
	gcb := gopointer.Restore(readArgs.UserData).(*PmdbServerObject)

	var ret int64
	if reqType == PumiceDBCommon.APP_REQ {
		//Calling the golang Application's Read function.
		ret = gcb.PmdbAPI.Read(&readArgs)
	} else if reqType == PumiceDBCommon.LEASE_REQ {
		//Calling leaseAPP Read
		ret = gcb.LeaseAPI.Read(&readArgs)
	}
	return ret
}

//export goInit
func goInit(args *C.struct_pumicedb_cb_cargs) {

	var initArgs PmdbCbArgs
	pmdbCbArgsInit(args, &initArgs)

	//Restore the golang function pointers stored in PmdbCallbacks.
	gcb := gopointer.Restore(initArgs.UserData).(*PmdbServerObject)

	gcb.PmdbAPI.Init(&initArgs)
	if gcb.LeaseEnabled {
		gcb.LeaseAPI.Init(&initArgs)
	}
}

/**
 * Start the pmdb server.
 * @raft_uuid: Raft UUID.
 * @peer_uuid: Peer UUID.
 * @cf: Column Family
 * @cb: PmdbAPI callback funcs.
 */
func PmdbStartServer(pso *PmdbServerObject) error {

	if pso == nil {
		return errors.New("Null server object parameter")
	}
	if pso.RaftUuid == "" || pso.PeerUuid == "" {
		return errors.New("Raft and/or peer UUIDs were not specified")
	}

	/*
	 * Convert the raft_uuid and peer_uuid go strings into C strings
	 * so that we can pass these to C function.
	 */

	raft_uuid_c := GoToCString(pso.RaftUuid)
	defer FreeCMem(raft_uuid_c)

	peer_uuid_c := GoToCString(pso.PeerUuid)
	defer FreeCMem(peer_uuid_c)

	cCallbacks := C.struct_PmdbAPI{}

	//Assign the callback functions
	cCallbacks.pmdb_apply = C.pmdb_apply_sm_handler_t(C.applyCgo)
	cCallbacks.pmdb_read = C.pmdb_read_sm_handler_t(C.readCgo)
	cCallbacks.pmdb_write_prep = C.pmdb_write_prep_sm_handler_t(C.writePrepCgo)
	cCallbacks.pmdb_init = C.pmdb_init_sm_handler_t(C.initCgo)

	/*
	 * Store the column family name into char * array.
	 * Store gostring to byte array.
	 * Don't forget to append the null terminating character.
	 */

	var i int
	cf_name := make(charsSlice, len(pso.ColumnFamilies))
	for _, cFamily := range pso.ColumnFamilies {
		cf_byte_arr := []byte(cFamily + "\000")
		cf_name[i] = (*C.char)(C.CBytes(cf_byte_arr))
		i = i + 1
	}

	//Convert Byte array to char **
	sH := (*reflect.SliceHeader)(unsafe.Pointer(&cf_name))
	cf_array := (**C.char)(unsafe.Pointer(sH.Data))

	// Create an opaque C pointer for cbs to pass to PmdbStartServer.
	opa_ptr := gopointer.Save(pso)
	defer gopointer.Unref(opa_ptr)

	// Starting the pmdb server.
	rc := C.PmdbExec(raft_uuid_c, peer_uuid_c, &cCallbacks, cf_array,
		C.int(len(pso.ColumnFamilies)), (C.bool)(pso.SyncWrites),
		(C.bool)(pso.CoalescedWrite), opa_ptr)

	if rc != 0 {
		return fmt.Errorf("PmdbExec() returned %d", rc)
	}

	return nil
}

// Method version of PmdbStartServer()
// writes metadata to 'path' while exiting
func (pso *PmdbServerObject) Run() error {
	go PumiceDBCommon.HandleKillSignal()
	return PmdbStartServer(pso)
}

// Export the common decode method via the server object
//TODO: Remove it from PmdbServerObject
func (*PmdbServerObject) Decode(input unsafe.Pointer, output interface{},
	len int64) error {
	return PumiceDBCommon.Decode(input, output, len)
}

func (*PmdbServerObject) DecodeApplicationReq(input []byte, output interface{}) error {
	dec := gob.NewDecoder(bytes.NewBuffer(input))
	for {
		if err := dec.Decode(output); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
	}

	return nil
}

func Decode(input unsafe.Pointer, output interface{},
	len int64) error {
	return PumiceDBCommon.Decode(input, output, len)
}

// search a key in RocksDB
func PmdbLookupKey(key string, key_len int64,
	go_cf string) ([]byte, error) {

	var goerr string
	var C_value_len C.size_t
	var result []byte
	var lookup_err error

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

	C.rocksdb_readoptions_destroy(ropts)

	if C_value != nil {

		buffer_value := CToGoBytes(C_value, C.int(C_value_len))
		result = C.GoBytes(unsafe.Pointer(C_value), C.int(C_value_len))
		log.Debug("C_value: ", C_value, " \nvalBytes: ", string(buffer_value))
		lookup_err = nil
		FreeCMem(C_value)
	} else {
		lookup_err = errors.New("Failed to lookup for key")
	}

	//Free C memory
	FreeCMem(err)
	FreeCMem(cf)
	FreeCMem(C_key)
	log.Trace("Result is :", result)
	return result, lookup_err
}

// Public method of PmdbLookupKey
func (*PmdbServerObject) LookupKey(key string, key_len int64,
	go_cf string) ([]byte, error) {
	return PmdbLookupKey(key, key_len, go_cf)
}

func PmdbWriteKV(app_id unsafe.Pointer, pmdb_handle unsafe.Pointer, key string,
	key_len int64, value string, value_len int64, gocolfamily string) int {

	//typecast go string to C char *
	cf := GoToCString(gocolfamily)

	C_key := GoToCString(key)
	log.Trace("Writing key to db :", key)
	C_key_len := GoToCSize_t(key_len)

	C_value := GoToCString(value)
	log.Trace("Writing value to db :", value)

	C_value_len := GoToCSize_t(value_len)

	capp_id := (*C.struct_raft_net_client_user_id)(app_id)

	cf_handle := C.PmdbCfHandleLookup(cf)

	//Calling pmdb library function to write Key-Value.
	rc := C.PmdbWriteKV(capp_id, pmdb_handle, C_key, C_key_len, C_value, C_value_len, nil, unsafe.Pointer(cf_handle))
	seqNum := int64(C.rocksdb_get_latest_sequence_number(C.PmdbGetRocksDB()))
	log.Trace("Seq Num for this write is - ", seqNum)
	go_rc := int(rc)
	if go_rc != 0 {
		log.Error("PmdbWriteKV failed with error: ", go_rc)
	}
	//Free C memory
	FreeCMem(cf)
	FreeCMem(C_key)
	FreeCMem(C_value)
	return go_rc
}

// Public method of PmdbWriteKV
func (*PmdbServerObject) WriteKV(app_id unsafe.Pointer,
	pmdb_handle unsafe.Pointer, key string,
	key_len int64, value string, value_len int64, gocolfamily string) int {

	return PmdbWriteKV(app_id, pmdb_handle, key, key_len, value, value_len,
		gocolfamily)
}

func PmdbReadKV(app_id unsafe.Pointer, key string,
	key_len int64, gocolfamily string) ([]byte, error) {

	go_value, err := PmdbLookupKey(key, key_len, gocolfamily)

	//Get the result
	return go_value, err
}

// Public method of PmdbReadKV
func (*PmdbServerObject) ReadKV(app_id unsafe.Pointer, key string,
	key_len int64, gocolfamily string) ([]byte, error) {

	return PmdbReadKV(app_id, key, key_len, gocolfamily)
}

// Methods for range iterator

// Wrapper for rocksdb_iter_seek -
// Seeks the passed iterator to the passed key or first key
func seekTo(key string, key_len int64, itr *C.rocksdb_iterator_t) {
	var cKey *C.char
	var cLen C.size_t

	if key == "" {
		C.rocksdb_iter_seek_to_first(itr)
	} else {
		cKey = GoToCString(key)
		cLen = GoToCSize_t(key_len)
		C.rocksdb_iter_seek(itr, cKey, cLen)
		FreeCMem(cKey)
	}
}

// Wrapper for rocksdb_iter_key/val -
// Returns the key and value from the where
// the iterator is present
func getKeyVal(itr *C.rocksdb_iterator_t) (string, []byte) {
	var cKeyLen C.size_t
	var cValLen C.size_t

	C_key := C.rocksdb_iter_key(itr, &cKeyLen)
	C_value := C.rocksdb_iter_value(itr, &cValLen)

	keyBytes := CToGoBytes(C_key, C.int(cKeyLen))
	valueBytes := CToGoBytes(C_value, C.int(cValLen))

	return string(keyBytes), valueBytes
}

func createRopts(consistent bool, seqNum *uint64) (*C.rocksdb_readoptions_t, bool) {
	var snapMiss bool
	var ropts *C.rocksdb_readoptions_t
	var retSeqNum C.ulong

	//Create ropts based on consistency requirement
	if consistent {
		ropts = C.PmdbGetRoptionsWithSnapshot(C.ulong(*seqNum), &retSeqNum)
		if *seqNum != CToGoUint64(retSeqNum) {
			if *seqNum != math.MaxUint64 {
				snapMiss = true
			}
			*seqNum = CToGoUint64(retSeqNum)
		}
	} else {
		ropts = C.rocksdb_readoptions_create()
	}

	return ropts, snapMiss
}

func destroyRopts(seqNum uint64, ropts *C.rocksdb_readoptions_t, consistent bool) {
	log.Trace("RangeQuery - Destroying ropts")
	if consistent {
		C.PmdbPutRoptionsWithSnapshot(C.ulong(seqNum))
	} else {
		C.rocksdb_readoptions_destroy(ropts)
	}
}

func pmdbFetchAllKV(key string, key_len int64, bufSize int64, go_cf string) (map[string][]byte, string, error) {
	var lookup_err error
	var resultMap = make(map[string][]byte)
	var mapSize int
	var lastKey string
	var itr *C.rocksdb_iterator_t

	log.Trace("Read All KV from column Family ", go_cf)

	//Create ropts
	ropts, _ := createRopts(false, nil)

	// create iterator
	cf := GoToCString(go_cf)
	cf_handle := C.PmdbCfHandleLookup(cf)
	itr = C.rocksdb_create_iterator_cf(C.PmdbGetRocksDB(), ropts, cf_handle)

	//Seek to the provided key
	seekTo(key, key_len, itr)

	// Iterate over keys store them in map if prefix
	for C.rocksdb_iter_valid(itr) != 0 {
		fKey, fVal := getKeyVal(itr)
		log.Trace("ReadAll key : ", fKey)

		// check if the key-val can be stored in the buffer
		entrySize := len([]byte(fKey)) + len([]byte(fVal)) + encodingOverhead
		if (bufSize > 0) && ((int64(mapSize) + int64(entrySize)) > bufSize) {
			log.Trace("ReadAll -  Reply buffer is full - dumping map to client")
			lastKey = fKey
			break
		}
		mapSize = mapSize + entrySize + encodingOverhead
		resultMap[fKey] = fVal

		C.rocksdb_iter_next(itr)
	}

	destroyRopts(0, ropts, false)

	//Free the iterator and memory
	C.rocksdb_iter_destroy(itr)
	FreeCMem(cf)

	if len(resultMap) == 0 {
		lookup_err = errors.New("No keys in the column family")
	} else {
		lookup_err = nil
	}
	return resultMap, lastKey, lookup_err
}

func pmdbFetchRange(key string, key_len int64,
	prefix string, bufSize int64, consistent bool, seqNum uint64, go_cf string) (map[string][]byte, string, uint64, bool, error) {
	var lookup_err error
	var resultMap = make(map[string][]byte)
	var mapSize int
	var lastKey string
	var itr *C.rocksdb_iterator_t
	var endReached bool

	log.Trace("RangeQuery - Key passed is: ", key, " Prefix passed is : ", prefix,
		" Seq No passed is : ", seqNum)

	//Create ropts based on consistency and seqNum
	ropts, snapMiss := createRopts(consistent, &seqNum)

	// create iterator
	cf := GoToCString(go_cf)
	cf_handle := C.PmdbCfHandleLookup(cf)
	itr = C.rocksdb_create_iterator_cf(C.PmdbGetRocksDB(), ropts, cf_handle)

	//Seek to the provided key
	seekTo(key, key_len, itr)

	// Iterate over keys store them in map if prefix
	for C.rocksdb_iter_valid(itr) != 0 {
		fKey, fVal := getKeyVal(itr)
		log.Trace("RangeQuery - Seeked to : ", fKey)

		// check if passed key is prefix of fetched key or exit
		if !strings.HasPrefix(fKey, prefix) {
			endReached = true
			break
		}

		// check if the key-val can be stored in the buffer
		entrySize := len([]byte(fKey)) + len([]byte(fVal)) + encodingOverhead
		if (int64(mapSize) + int64(entrySize)) > bufSize {
			log.Trace("RangeQuery -  Reply buffer is full - dumping map to client")
			lastKey = fKey
			break
		}
		mapSize = mapSize + entrySize + encodingOverhead
		resultMap[fKey] = fVal

		C.rocksdb_iter_next(itr)
	}

	//Destroy ropts for consistent mode only when reached the end of the range query
	//Wheras, destroy ropts in every iteration if the range query is not consistent
	if C.rocksdb_iter_valid(itr) == 0 || endReached == true {
		destroyRopts(seqNum, ropts, consistent)
	} else if !consistent {
		destroyRopts(seqNum, ropts, consistent)
	}

	//Free the iterator and memory
	C.rocksdb_iter_destroy(itr)
	FreeCMem(cf)

	if len(resultMap) == 0 {
		lookup_err = errors.New("Failed to lookup for key")
	} else {
		lookup_err = nil
	}
	return resultMap, lastKey, seqNum, snapMiss, lookup_err
}

// Public method for read all KV from the column family
func (*PmdbServerObject) ReadAllKV(app_id unsafe.Pointer, key string,
	key_len int64, bufSize int64, gocolfamily string) (map[string][]byte, string, error) {

	return pmdbFetchAllKV(key, key_len, bufSize, gocolfamily)
}

// Public method for range read KV
func (*PmdbServerObject) RangeReadKV(app_id unsafe.Pointer, key string,
	key_len int64, prefix string, bufSize int64, consistent bool, seqNum uint64, gocolfamily string) (map[string][]byte, string, uint64, bool, error) {

	return pmdbFetchRange(key, key_len, prefix, bufSize, consistent, seqNum, gocolfamily)
}

// Copy data from the user's application into the pmdb reply buffer
func PmdbCopyDataToBuffer(ed interface{}, buffer unsafe.Pointer) (int64, error) {
	var key_len int64
	//Encode the structure into void pointer.
	encoded_key, err := PumiceDBCommon.Encode(ed, &key_len)
	if err != nil {
		log.Print("Failed to encode data during copy data: ", err)
		return -1, err
	}

	//Copy the encoded structed into buffer
	C.memcpy(buffer, encoded_key, C.size_t(key_len))

	return key_len, nil
}

// Public method version of PmdbCopyDataToBuffer
func (*PmdbServerObject) CopyDataToBuffer(ed interface{},
	buffer unsafe.Pointer) (int64, error) {
	return PmdbCopyDataToBuffer(ed, buffer)
}

func (*PmdbServerObject) GetCurrentHybridTime() float64 {
	//TODO: Update this code
	return 0.0
}

// Get the leader timestamp.
func PmdbGetLeaderTimeStamp(ts *PmdbLeaderTS) error {

	var ts_c C.struct_raft_leader_ts
	err := errors.New("Not a leader")

	rc := C.PmdbGetLeaderTimeStamp(&ts_c)

	rc_go := int(rc)
	if rc_go == 0 {
		ts.Term = int64(ts_c.rlts_term)
		ts.Time = int64(ts_c.rlts_time)
		err = nil
	}

	return err
}

func PmdbIsLeader() bool {
	rc := C.PmdbIsLeader()
	return bool(rc)
}

func PmdbInitRPCMsg(rcm *C.struct_raft_client_rpc_msg, dataSize uint32) {
	rcm.rcrm_type = C.RAFT_CLIENT_RPC_MSG_TYPE_WRITE
	rcm.rcrm_version = 0
	rcm.rcrm_data_size = C.uint32_t(dataSize)
}

func PmdbEnqueueDirectWriteRequest(appReq interface{}) int {
	var appBuf bytes.Buffer
	var req PumiceDBCommon.PumiceRequest
	var rncui_id C.struct_raft_net_client_user_id
	var obj_id *C.pmdb_obj_id_t

	//Encode the application request structure.
	enc := gob.NewEncoder(&appBuf)
	err := enc.Encode(appReq)
	if err != nil {
		log.Error("Failed to encode the stale lease processing request")
		return -1
	}

	uuid := uuid.NewV4().String()
	req.Rncui = fmt.Sprintf("%s:0:0:0:0", uuid)
	req.ReqType = PumiceDBCommon.LEASE_REQ
	req.ReqPayload = appBuf.Bytes()

	//Encode the PumiceRequest
	var reqBuf bytes.Buffer
	pumiceEnc := gob.NewEncoder(&reqBuf)
	err = pumiceEnc.Encode(req)
	if err != nil {
		log.Error(err)
		return -1
	}

	data := reqBuf.Bytes()

	//Prepare rncui c string
	rncuiStrC := GoToCString(req.Rncui)
	defer FreeCMem(rncuiStrC)

	//Convert it to unsafe pointer (void * for C function)
	kvdata := unsafe.Pointer(&data[0])

	dsize := int64(len(data))
	rmsize := C.sizeof_struct_raft_client_rpc_msg
	pmsize := C.sizeof_struct_pmdb_msg
	pmdSize := C.sizeof_struct_pmdb_msg + C.int64_t(dsize)

	//total size of the request buffer
	totalSize := int64(rmsize) + int64(pmsize) + dsize
	buf := C.malloc(C.size_t(totalSize))

	//Populate raft_client_rpc_msg structure
	rcm := (*C.struct_raft_client_rpc_msg)(buf)
	PmdbInitRPCMsg(rcm, uint32(pmdSize))

	// Get the pointer for rcrm_data
	rptr := unsafe.Pointer(uintptr(buf) + C.sizeof_struct_raft_client_rpc_msg)

	//Prepare pmdb_obj_id
	C.pumice_server_rncui_id_parse(rncuiStrC, &rncui_id, 0)

	obj_id = (*C.pmdb_obj_id_t)(&rncui_id.rncui_key)

	//Populate pmdb_msg structure
	pmdb_msg := (*C.struct_pmdb_msg)(rptr)
	C.pmdb_direct_msg_init(pmdb_msg, obj_id, C.uint32_t(dsize), C.pmdb_op_write, 0)

	//Get the pointer to pmdbrm_data and store the PumiceRequest
	dataPtr := unsafe.Pointer(uintptr(rptr) + C.sizeof_struct_pmdb_msg)
	C.memcpy(dataPtr, kvdata, C.size_t(dsize))

	//Enqueue the direct request
	ret := C.raft_server_enq_direct_raft_req_from_leader((*C.char)(buf), C.int64_t(totalSize))
	C.free(buf)
	
	return int(ret)
}
