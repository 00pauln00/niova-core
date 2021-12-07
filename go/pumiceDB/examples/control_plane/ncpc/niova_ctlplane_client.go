package main

import (
	"time"
        "flag"
        "fmt"
        PumiceDBCommon "niova/go-pumicedb-lib/common"
	"ctlplane/clientapi"
	"encoding/gob"
        "ctlplane/niovakvlib"
        "os"
	"bytes"
        log "github.com/sirupsen/logrus"

)

type ncp_client struct {
        reqKey string
	reqValue string
	addr string
	operation string
	configPath string
	logPath string
	ncpc clientapi.ClientAPI
}

func usage() {
        flag.PrintDefaults()
        os.Exit(0)
}

//Function to get command line parameters
func (cli *ncp_client) getCmdParams() {
        flag.StringVar(&cli.reqKey, "k", "Key", "Key prefix")
        flag.StringVar(&cli.reqValue, "v", "Value", "Value prefix")
        flag.StringVar(&cli.configPath, "c", "./gossipConfig", "Raft peer config")
        flag.StringVar(&cli.logPath , "l", "/tmp/temp.log", "Log path")
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
	time.Sleep(6*time.Second)
	//Send request
        var write bool
	requestObj := niovakvlib.NiovaKV{}

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
		response := clientObj.ncpc.Request(request.Bytes(), "", write)
		fmt.Println("Response:", string(response))

	case "config":
		clientObj.ncpc.Request([]byte(clientObj.reqKey), "/config", false)
		fmt.Println("Response : ", string(response))
        }

	clientObj.ncpc.DumpIntoJson("./")

}
