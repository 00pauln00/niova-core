package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
	"niova/go-pumicedb-lib/client"
	"niovactlplane/niovareqlib"
	"os"
	"strings"
)

var (
	raftUuid     string
	clientUuid   string
	key          string
	value        []byte
	jsonOutFpath string
)

//Structure definition for client.
type niovaCtlClient struct {
	clientObj *PumiceDBClient.PmdbClientObj
	reqObj    *niovareqlib.NiovaCtlReq
	rncui     string
}

//Function to initialize logger.
func initLogger() error {
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
func (nco *niovaCtlClient) Write() error {

	var errorMsg error
	//Perform write operation.
	err := nco.clientObj.Write(nco.reqObj, nco.rncui)
	if err != nil {
		log.Error("Write key-value failed : ", err)
		errorMsg = errors.New("exec method for WriteOne Operation failed.")
	} else {
		log.Info("Pmdb Write successful!")
		errorMsg = nil
	}
	return errorMsg
}

//Method to perform read operation.
func (nco *niovaCtlClient) Read() error {

	var rErr error
	rop := &niovareqlib.NiovaCtlReq{}
	err := nco.clientObj.Read(nco.reqObj, nco.rncui, rop)
	if err != nil {
		log.Error("Read request failed !!", err)
		rErr = errors.New("exec method for ReadOne Operation failed")
	} else {
		log.Info("Result of the read request is:", rop)
	}
	return rErr
}

//Function to get command line parameters while starting of the client.
func getCmdParams() {
	flag.StringVar(&raftUuid, "r", "NULL", "raft uuid")
	flag.StringVar(&clientUuid, "u", "NULL", "client uuid")
	flag.StringVar(&jsonOutFpath, "l", "./", "json_outfilepath")

	flag.Parse()
	fmt.Println("Raft UUID: ", raftUuid)
	fmt.Println("Client UUID: ", clientUuid)
	fmt.Println("Json outfilepath:", jsonOutFpath)
}

func main() {

	//Get command line parameters.
	getCmdParams()

	//Initialize logger.
	logErr := initLogger()
	if logErr != nil {
		log.Error(logErr)
	}

	//Generate uuid for temporary json file.
	appUuid := uuid.NewV4().String()
	rncui := appUuid + ":0:0:0:0"

	//Create new client object.
	clientObj := PumiceDBClient.PmdbClientNew(raftUuid, clientUuid)
	if clientObj == nil {
		return
	}
	log.Info("Starting client: ", clientUuid)
	//Start the client.
	clientObj.Start()
	defer clientObj.Stop()

	for {
		fmt.Print("\nEnter operation, key and value: ")
		input := bufio.NewReader(os.Stdin)
		cmd, _ := input.ReadString('\n')
		cmdS := strings.Replace(cmd, "\n", "", -1)
		cmdParams := strings.Split(cmdS, "#")
		ops := cmdParams[0]

		switch ops {

		case "write":
			key = cmdParams[1]
			value = []byte(cmdParams[2])
			reqObj := niovareqlib.NiovaCtlReq{
				InputKey:   key,
				InputValue: value,
			}
			ncc := niovaCtlClient{clientObj, &reqObj, rncui}
			err := ncc.Write()
			if err != nil {
				log.Error(err)
			}

		case "read":
			key = cmdParams[1]
			reqObj := niovareqlib.NiovaCtlReq{
				InputKey: key,
			}
			ncc := niovaCtlClient{clientObj, &reqObj, rncui}
			err := ncc.Read()
			if err != nil {
				log.Error(err)
			}

		case "exit":
			os.Exit(0)
		default:
			fmt.Print("\nEnter valid operation....")
		}

	}
}
