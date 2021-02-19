package DictAppLib
import (
	"fmt"
	"unsafe"
	"encoding/gob"
	"log"
	"bytes"
	"io"
)

/*
#include <stdlib.h>
*/
import "C"

type Dict_request struct {
	Dict_op string
	Dict_wr_seq uint64
	Dict_rncui string
	Dict_text string
	Dict_wcount int
}


func DictAppDecodebuf(input_buf unsafe.Pointer, bufsz int64)  *Dict_request{
	gob.Register(Dict_request{})

	input_bytes_data := C.GoBytes(unsafe.Pointer(input_buf), C.int(bufsz))

	buffer := bytes.NewBuffer(input_bytes_data)

	dict_req := &Dict_request{}
	dec := gob.NewDecoder(buffer)
	for {
		if err := dec.Decode(dict_req); err == io.EOF {
			fmt.Println("EOF reached, break from the loop")
			break
		} else if err != nil {
			log.Fatal(err)
		}
	}

	return dict_req
}
