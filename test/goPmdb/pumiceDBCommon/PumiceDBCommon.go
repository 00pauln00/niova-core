package PumiceDBCommon
import (
	"unsafe"
	"encoding/gob"
	"bytes"
	"log"
	"io"
)

/*
#include <stdlib.h>
#include <stdio.h>
*/
import "C"

func Encode(ed interface{}, data_len *int64) unsafe.Pointer {
	//Byte array
	buffer := bytes.Buffer{}

	encode := gob.NewEncoder(&buffer)
	err := encode.Encode(ed)
	if err != nil {
		log.Fatal(err)
	}

	struct_data := buffer.Bytes()
	*data_len = int64(len(struct_data))

	//Convert it to unsafe pointer (void * for C function)
	//enc_data := (*C.char)(unsafe.Pointer(&struct_data[0]))
	enc_data := unsafe.Pointer(&struct_data[0])

	return enc_data
}

/*
 * Get the actual size of the structure by converting it to byte array.
 */
func GetStructSize(ed interface{}) int64 {
	var struct_size int64
	Encode(ed, &struct_size)

	return struct_size
}

func Decode(input unsafe.Pointer, output interface{},
			data_len int64) {

	bytes_data := C.GoBytes(unsafe.Pointer(input), C.int(data_len))

	buffer := bytes.NewBuffer(bytes_data)

	dec := gob.NewDecoder(buffer)
	for {
		if err := dec.Decode(output); err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}
	}
}

func GoPmdbDecoder(ed interface{}, buffer_ptr unsafe.Pointer, buf_size int64) {
	data := C.GoBytes(unsafe.Pointer(buffer_ptr), C.int(buf_size))
	byte_arr := bytes.NewBuffer(data)

	decode := gob.NewDecoder(byte_arr)
	for {
		if err := decode.Decode(ed); err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}
	}
}
