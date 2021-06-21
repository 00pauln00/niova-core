package PumiceDBCommon

import (
	"bytes"
	"encoding/gob"
	"io"
	"unsafe"
)

/*
#include <stdlib.h>
#include <stdio.h>
*/
import "C"

type PMDBInfo struct {
        RaftUUID   string
        ClientUUID string
        LeaderUUID string
}

//Encode the data passed as interface and return the unsafe.Pointer
// to the encoded data. Also return length of the encoded data.
func Encode(ed interface{}, data_len *int64) (unsafe.Pointer, error) {
	//Byte array
	buffer := bytes.Buffer{}

	encode := gob.NewEncoder(&buffer)
	err := encode.Encode(ed)
	if err != nil {
		return nil, err
	}

	struct_data := buffer.Bytes()
	*data_len = int64(len(struct_data))

	//Convert it to unsafe pointer (void * for C function)
	enc_data := unsafe.Pointer(&struct_data[0])

	return enc_data, nil
}

/*
 * Get the actual size of the structure by converting it to byte array.
 */
func GetStructSize(ed interface{}) int64 {
	var struct_size int64
	Encode(ed, &struct_size)

	return struct_size
}

//Decode the data in user specific structure.
func Decode(input unsafe.Pointer, output interface{},
	data_len int64) error {

	bytes_data := C.GoBytes(unsafe.Pointer(input), C.int(data_len))

	buffer := bytes.NewBuffer(bytes_data)

	dec := gob.NewDecoder(buffer)
	for {
		if err := dec.Decode(output); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
	}

	return nil
}
