package main
import (
	"fmt"
	"unsafe"
)
/*
#cgo pkg-config: niova --define-variable=prefix=/home/manisha/binaries/niova/
#include <raft/pumice_db.h>
extern void applyCgo(const struct raft_net_client_user_id *, const char *,
                     size_t, void *, void *);
extern void readCgo(const struct raft_net_client_user_id *, const char *,
                    size_t, char *, size_t, void *);
*/
import "C"
import gopointer "github.com/mattn/go-pointer"

type GoApplyCallback func(*C.struct_raft_net_client_user_id, *C.char, C.size_t,
                          unsafe.Pointer)
type GoReadCallback func(*C.struct_raft_net_client_user_id, *C.char, C.size_t,
                         *C.char, C.size_t)

type GoCallbacks struct {
	applyCb GoApplyCallback
	readCb GoReadCallback
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
func goApply(app_id *C.struct_raft_net_client_user_id, input_buf *C.char,
			input_buf_sz C.size_t, pmdb_handle unsafe.Pointer,
            user_data unsafe.Pointer) {
	fmt.Println("Inside Go apply and now calling that function")
	gcb := gopointer.Restore(user_data).(*GoCallbacks)
	gcb.applyCb(app_id, input_buf, input_buf_sz, pmdb_handle)
}

//export goRead
func goRead(app_id *C.struct_raft_net_client_user_id, request_buf *C.char,
            request_bufsz C.size_t, reply_buf *C.char, reply_bufsz C.size_t,
            user_data unsafe.Pointer) {
	fmt.Println("Inside Go Read and now calling that function")
	gcb := gopointer.Restore(user_data).(*GoCallbacks)
	gcb.readCb(app_id, request_buf, request_bufsz, reply_buf, reply_bufsz)
}

func myapply(app_id *C.struct_raft_net_client_user_id, input_buf *C.char,
			input_buf_sz C.size_t, pmdb_handle unsafe.Pointer) {
	fmt.Println("from go myapply")
}

func myread(app_id *C.struct_raft_net_client_user_id, request_buf *C.char,
            request_bufsz C.size_t, reply_buf *C.char, reply_bufsz C.size_t) {
	fmt.Println("from go myread")
}

func main() {
	fmt.Println("Inside go main")
	cb := &GoCallbacks{
		applyCb: myapply,
		readCb:  myread,
	}
	GoTraverse(cb)
}
