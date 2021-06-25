package niovakvpmdbclient

import (
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"niova/go-pumicedb-lib/client"
	"niovakv/niovakvlib"
	"os"
)

//Structure definition for client.
type NiovaKVClient struct {
	ClientObj *PumiceDBClient.PmdbClientObj
	ReqObj    *niovakvlib.NiovaKV
	Rncui     string
}

var num_req int

//Function to initialize logger.
func initLogger(clientUuid, jsonOutFpath string) error {
	var filename string = jsonOutFpath + "/" + clientUuid + ".log"
	// Create the log file if doesn't exist. And append to it if it already exists.
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		log.Error(err)
		return err
	}
	Formatter := new(log.TextFormatter)
	//Set Formatter.
	Formatter.TimestampFormat = "02-01-2006 15:04:05"
	Formatter.FullTimestamp = true
	log.SetFormatter(Formatter)
	if err != nil {
		// Cannot open log file. Logging to stderr
		log.Error(err)
	} else {
		log.SetOutput(f)
	}
	log.Info("client uuid:", clientUuid)
	return err
}

//Method to perform write operation.
func (nco *NiovaKVClient) Write() error {

	var errorMsg error
	//Perform write operation.
	rncui := fmt.Sprintf("%s:0:0:0:%d", nco.Rncui, num_req)
	err := nco.ClientObj.Write(nco.ReqObj, rncui)
	if err != nil {
		log.Error("Write key-value failed : ", err)
		errorMsg = errors.New("Write operation failed.")
	} else {
		log.Info("Pmdb Write successful!")
		errorMsg = nil
	}
	num_req = num_req + 1
	return errorMsg
}

//Method to perform read operation.
func (nco *NiovaKVClient) Read() ([]byte, error) {

	var rErr error
	rop := &niovakvlib.NiovaKV{}
	err := nco.ClientObj.Read(nco.ReqObj, nco.Rncui, rop)
	if err != nil {
		log.Error("Read request failed !!", err)
		rErr = errors.New("Read operation failed")
	} else {
		log.Info("Result of the read request is:", rop)
	}
	return rop.InputValue, rErr
}

//Function to get pumicedb client object.
func GetNiovaKVClientObj(raftUuid, clientUuid, jsonOutFpath string) *NiovaKVClient {

	//Initialize logger.
	logErr := initLogger(clientUuid, jsonOutFpath)
	if logErr != nil {
		log.Error(logErr)
	}

	//Create new client object.
	clientObj := PumiceDBClient.PmdbClientNew(raftUuid, clientUuid)
	if clientObj == nil {
		return nil
	}
	ncc := &NiovaKVClient{}
	ncc.ClientObj = clientObj
	return ncc
}

//Function to perform operations.
func (nkvClient *NiovaKVClient) ProcessRequest() ([]byte, int) {

	var (
		value  []byte
		status int
	)

	ops := nkvClient.ReqObj.InputOps
	switch ops {

	case "write":
		err := nkvClient.Write()
		if err != nil {
			log.Error(err)
			status = -1
		} else {
			fmt.Println("Write operation successful")
			status = 0
		}

	case "read":
		rval, err := nkvClient.Read()
		if err != nil {
			log.Error(err)
			status = -1
		} else {
			value = rval
			log.Info("Data received after read request:", value)
		}

	case "exit":
		os.Exit(0)
	default:
		fmt.Print("\nEnter valid operation....")
	}

	return value, status
}
