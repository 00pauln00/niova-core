package PumiceDBCommon

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"runtime/coverage"
	"strings"
	"syscall"
	"unsafe"

	log "github.com/sirupsen/logrus"
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

type PumiceRequest struct {
	Rncui      string
	ReqType    int
	ReqPayload []byte
}

const (
	APP_REQ     int = 0
	LEASE_REQ       = 1
	LOOKOUT_REQ     = 2
)

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

//emit code coverage data to the path file if it exists
func EmitCoverData(path string) {
	log.Info("Writing code coverage data to : ", path)
	if err := coverage.WriteMetaDir(path); err != nil {
		log.Error("Error while writing cover meta dir : ", err)
	}
	if err := coverage.WriteCountersDir(path); err != nil {
		log.Error("error while writing counter metadata : ", err)
	}
}

// catch SIGTERM, emit cover data to path, exit
func HandleKillSignal() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM)
	go func() {
		<-sigs
		log.Info("Received SIGTERM")
		path := os.Getenv("GOCOVERDIR")
		if path == "" {
			path = "/tmp/niova-core/go-code-cov-" + string(os.Getpid())
		}
		EmitCoverData(path)
		os.Exit(1)
	}()
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

//Prepare PumiceRequest for application reqest type APP_REQ
func PrepareAppPumiceRequest(appRequest interface{}, rncui string, requestBytes *bytes.Buffer) error {
	var b bytes.Buffer
	var rqo PumiceRequest

	//Encode the application request to prepare payload
	enc := gob.NewEncoder(&b)
	err := enc.Encode(appRequest)
	if err != nil {
		log.Error("Encoding error : ", err)
		return err
	}

	rqo.ReqType = APP_REQ
	rqo.ReqPayload = b.Bytes()
	rqo.Rncui = rncui

	//Encode PumiceRequest
	pumiceEnc := gob.NewEncoder(requestBytes)
	err = pumiceEnc.Encode(rqo)
	if err != nil {
		log.Error("Encoding error : ", err)
	}
	return err
}
