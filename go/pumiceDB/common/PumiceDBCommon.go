package PumiceDBCommon

import (
	"bytes"
	"encoding/gob"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"strings"
	"strconv"
	"common/serfAgent"
	"unsafe"
	"net"
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

type PeerConfigData struct {
        UUID       [16]byte
        IPAddr     net.IP
        Port       uint16
        ClientPort uint16
}

type PMDBGossipInfo struct {
	Status	  bool
	State 	  int
}

//Func for initializing the logger
func InitLogger(logPath string) error {

	// Split logpath name.
	parts := strings.Split(logPath, "/")
	fname := parts[len(parts)-1]
	dir := strings.TrimSuffix(logPath, fname)

	// Create directory if not exist.
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.MkdirAll(dir, 0700)
	}

	filename := dir + fname
	fmt.Println("logfile:", filename)

	//Create the log file if doesn't exist. And append to it if it already exists.
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	Formatter := new(log.TextFormatter)

	//Set Timestamp format for logfile.
	Formatter.TimestampFormat = "02-01-2006 15:04:05"
	Formatter.FullTimestamp = true
	log.SetFormatter(Formatter)

	if err != nil {
		// Cannot open log file. Logging to stderr
		log.Error(err)
	} else {
		log.SetOutput(f)
	}
	return err
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

func GetMemberGossipInfo(Handler *serfAgent.SerfAgentHandler) map[string]PMDBGossipInfo {
	memberInfo := make(map[string]PMDBGossipInfo)
	members := Handler.AgentObj.Serf().Members()
	for _, mems := range members {
		var temp PMDBGossipInfo
		if(mems.Status == 1){
			temp.Status = true
		} else {
			temp.Status = false
		}
		temp.State, _ = strconv.Atoi(mems.Tags["State"])
		memberInfo[mems.Name] = temp
	}
	fmt.Println(memberInfo)
	return memberInfo
}
