package main
import (
	"fmt"
	"os"
	"io"
	"unsafe"
	"strconv"
	"encoding/gob"
	"log"
	"bytes"
	"strings"
)

/*
#cgo pkg-config: niova --define-variable=prefix=/home/manisha/binaries/niova/
#include <raft/pumice_db.h>
#include <rocksdb/c.h>
#include <raft/raft_net.h>
extern void applyCgo(const struct raft_net_client_user_id *, const void *,
                     size_t, void *, void *);
extern void readCgo(const struct raft_net_client_user_id *, const void *,
                    size_t, void *, size_t, void *);
*/
import "C"
import gopointer "github.com/mattn/go-pointer"

type GoApplyCallback func(*C.struct_raft_net_client_user_id, unsafe.Pointer, C.size_t,
                          unsafe.Pointer)
type GoReadCallback func(*C.struct_raft_net_client_user_id, unsafe.Pointer, C.size_t,
                         unsafe.Pointer, C.size_t)

type GoCallbacks struct {
	applyCb GoApplyCallback
	readCb GoReadCallback
}

var seqno = 0

type dict_request struct {
	Dict_op string
	Dict_wr_seq uint64
	Dict_rncui string
	Dict_text string
}

func GoTraverse(cbs *GoCallbacks) {
	fmt.Println("Inside GoTraverse")
	cCallbacks := C.struct_PmdbAPI{}

	if cbs.applyCb != nil {
		cCallbacks.pmdb_apply = C.pmdb_apply_sm_handler_t(C.applyCgo)
	}
	if cbs.readCb != nil {
		cCallbacks.pmdb_read = C.pmdb_read_sm_handler_t(C.readCgo)
	}

	//First parameter is RAFT_UUID
	ruuid := os.Args[1]

    fmt.Printf("Raft uuid: %s\n", ruuid)
	raft_uuid := C.CString(ruuid)
	defer C.free(unsafe.Pointer(raft_uuid))

	//Second parameter is PEER_UUID
	puuid := os.Args[2]
    fmt.Printf("Peer uuid: %s\n", puuid)

	peer_uuid := C.CString(puuid)
	defer C.free(unsafe.Pointer(peer_uuid))

	// Create an opaque C pointer for cbs to pass to traverse.
	p := gopointer.Save(cbs)
	defer gopointer.Unref(p)

	fmt.Println("Calling C function")
	C.PmdbExecGo(raft_uuid, peer_uuid, &cCallbacks, 1, true, p)
}

//export goApply
func goApply(app_id *C.struct_raft_net_client_user_id, input_buf unsafe.Pointer,
			input_buf_sz C.size_t, pmdb_handle unsafe.Pointer,
            user_data unsafe.Pointer) {
	fmt.Println("Inside Go apply and now calling that function")
	gcb := gopointer.Restore(user_data).(*GoCallbacks)
	gcb.applyCb(app_id, input_buf, input_buf_sz, pmdb_handle)
}

//export goRead
func goRead(app_id *C.struct_raft_net_client_user_id, request_buf unsafe.Pointer,
            request_bufsz C.size_t, reply_buf unsafe.Pointer, reply_bufsz C.size_t,
            user_data unsafe.Pointer) {
	fmt.Println("Inside Go Read and now calling that function")
	gcb := gopointer.Restore(user_data).(*GoCallbacks)
	gcb.readCb(app_id, request_buf, request_bufsz, reply_buf, reply_bufsz)
}

func myapply(app_id *C.struct_raft_net_client_user_id, input_buf unsafe.Pointer,
			input_buf_sz C.size_t, pmdb_handle unsafe.Pointer) {
	fmt.Println("from go myapply")
	seqno++

	fmt.Println("buf size %d", input_buf_sz)
	gob.Register(dict_request{})
	fmt.Println("Input buffer %s", input_buf)

	recv_data := C.GoBytes(unsafe.Pointer(input_buf), C.int(input_buf_sz))
	fmt.Println("Convert the bytes arr %s", string(recv_data))

	recv_bytes_arr := bytes.NewBuffer(recv_data)
	fmt.Println("New buffer %s", recv_bytes_arr)

	request := &dict_request{}
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

	//Create empty map
	word_mp := make(map[string]int)

    c := strings.Split(request.Dict_text, ",")
    justString := strings.Join(c," ")
    res := strings.Replace(justString, ".", " ",-1)
    res2 := strings.Replace(res, "(", " ",-1)
    data_str := strings.Replace(res2, ")", " ",-1)
    words := strings.Split(data_str, " ")

	cf := C.CString("PMDBTS_CF")
	defer C.free(unsafe.Pointer(cf))

	// Store words and its count in the map
    for _,word := range words{
        word_mp[word]++
    }

	for word, count := range word_mp {
		go_key_len := len(word)
		C_key := C.CString(word)
		defer C.free(unsafe.Pointer(C_key))
		C_key_len := C.size_t(go_key_len)

		value := strconv.Itoa(count)
		value_len := len(value)
		C_value := C.CString(value)
		defer C.free(unsafe.Pointer(C_value))
		C_value_len := C.size_t(value_len)

		C.PmdbWriteKVGo(app_id, pmdb_handle, C_key, C_key_len, C_value, C_value_len, nil, cf)

	}

}

func myread(app_id *C.struct_raft_net_client_user_id, request_buf unsafe.Pointer,
            request_bufsz C.size_t, reply_buf unsafe.Pointer, reply_bufsz C.size_t) {
	fmt.Println("from go myread")

	cf_name := C.CString("PMDBTS_CF")
	defer C.free(unsafe.Pointer(cf_name))

	recv_data := C.GoBytes(unsafe.Pointer(request_buf), C.int(request_bufsz))
	fmt.Println("Convert the bytes arr %s", string(recv_data))

	recv_bytes_arr := bytes.NewBuffer(recv_data)
	fmt.Println("New buffer %s", recv_bytes_arr)

	request := &dict_request{}
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

	C_req_string := C.CString(request.Dict_text)
	defer C.free(unsafe.Pointer(C_req_string))

	req_len := len(request.Dict_text)
	C_req_len := C.size_t(req_len)

	rc := C.Pmdb_test_app_lookup(app_id, C_req_string, C_req_len, cf_name)
	fmt.Println("Return value is", rc)
}

func main() {
	fmt.Println("Inside go main")
	cb := &GoCallbacks{
		applyCb: myapply,
		readCb:  myread,
	}
	GoTraverse(cb)
}
