package main

import (
	"bytes"
	"ctlplane/clientapi"
	"ctlplane/niovakvlib"
	"encoding/gob"
	"flag"
	"fmt"
	log "github.com/sirupsen/logrus"
	PumiceDBCommon "niova/go-pumicedb-lib/common"
	"os"
)

type ncp_client struct {
	reqKey     string
	reqValue   string
	addr       string
	operation  string
	configPath string
	logPath    string
	ncpc       clientapi.ClientAPI
}

func usage() {
	flag.PrintDefaults()
	os.Exit(0)
}

//Function to get command line parameters
func (cli *ncp_client) getCmdParams() {
	flag.StringVar(&cli.reqKey, "k", "Key", "Key prefix")
	flag.StringVar(&cli.reqValue, "v", "Value", "Value prefix")
	flag.StringVar(&cli.configPath, "c", "./gossipNodes", "Raft peer config")
	flag.StringVar(&cli.logPath, "l", "/tmp/temp.log", "Log path")
	flag.StringVar(&cli.operation, "o", "NULL", "Specify the opeation to perform")
	flag.Parse()
}

func main() {
	//Intialize client object
	clientObj := ncp_client{}

	//Get commandline parameters.
	clientObj.getCmdParams()
	flag.Usage = usage
	if flag.NFlag() == 0 {
		usage()
		os.Exit(-1)
	}

	//Create logger
	err := PumiceDBCommon.InitLogger(clientObj.logPath)
	if err != nil {
		log.Error("Error while initializing the logger  ", err)
	}

	log.Info("----START OF EXECUTION---")

	//Init niovakv client API
	clientObj.ncpc = clientapi.ClientAPI{
		Timeout: 10,
	}
	stop := make(chan int)
	go func() {
		err := clientObj.ncpc.Start(stop, clientObj.configPath)
		if err != nil {
			log.Error(err)
			os.Exit(1)
		}
	}()
	clientObj.ncpc.Till_ready()
	//time.Sleep(5 * time.Second)
	//Send request
	var write bool
	requestObj := niovakvlib.NiovaKV{}
	response := niovakvlib.NiovaKVResponse{}
	switch clientObj.operation {
	case "write":
		requestObj.InputValue = []byte(clientObj.reqValue)
		write = true
		fallthrough

	case "read":
		requestObj.InputKey = clientObj.reqKey
		requestObj.InputOps = clientObj.operation
		var request bytes.Buffer
		enc := gob.NewEncoder(&request)
		enc.Encode(requestObj)
		responseByteArray := clientObj.ncpc.Request(request.Bytes(), "", write)
		dec := gob.NewDecoder(bytes.NewBuffer(responseByteArray))
		err = dec.Decode(&response)
		fmt.Println("Response:", response)

	case "config":
		responseByteArray := clientObj.ncpc.Request([]byte(clientObj.reqKey), "/config", false)
		fmt.Println("Response : ", string(responseByteArray))
        }

	clientObj.ncpc.DumpIntoJson("./execution_summary.json")

}
