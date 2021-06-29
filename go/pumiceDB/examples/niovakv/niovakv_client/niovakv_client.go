package main

import (
	"flag"
	"math/rand"
	"niovakv/httpclient"
	"niovakv/niovakvlib"
	"niovakv/serfclienthandler"
	"os"

	log "github.com/sirupsen/logrus"
)

var (
	ClientHandler                               serfclienthandler.SerfClientHandler
	config_path, operation, key, value, logPath string
)

//Create logfile for client.
func initLogger() {

	var filename string = logPath + "/" + "niovakvClient" + ".log"

	//Create the log file if doesn't exist. And append to it if it already exists.i
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
}

//Function to get command line parameters while starting of the client.
func getCmdParams() {
	flag.StringVar(&config_path, "c", "./", "config file path")
	flag.StringVar(&logPath, "l", ".", "log file path")
	flag.StringVar(&operation, "o", "NULL", "write/read operation")
	flag.StringVar(&key, "k", "NULL", "Key")
	flag.StringVar(&value, "v", "NULL", "Value")
	flag.Parse()
}

//Get any client addr
func getServerAddr(refresh bool) (string, string) {
	if refresh {
		ClientHandler.GetData(true)
	}
	//Get random addr
	randomIndex := rand.Intn(len(ClientHandler.AgentAddrs))
	Addr := ClientHandler.AgentAddrs[randomIndex]
	return Addr, ClientHandler.Hport
}

func main() {

	//Get commandline parameters.
	getCmdParams()

	//Create log file.
	initLogger()

	//For serf client init
	ClientHandler = serfclienthandler.SerfClientHandler{}
	ClientHandler.Initdata(config_path)
	ClientHandler.Retries = 5
	ClientHandler.AgentData = make(map[string]*serfclienthandler.Data)

	reqObj := niovakvlib.NiovaKV{}
	if operation == "write" {
		reqObj.InputOps = operation
		reqObj.InputKey = key
		reqObj.InputValue = []byte(value)
	} else {
		reqObj.InputOps = operation
		reqObj.InputKey = key
	}

	//Do upto 5 times if request failed
	refresh := false
	for i := 0; i < 5; i++ {
		// Get the alive http server IP and port
		addr, port := getServerAddr(refresh)
		//Send the request over http
		err := httpclient.SendRequest(&reqObj, addr, port)
		if err == nil {
			break
		}
		refresh = true
	}
}
