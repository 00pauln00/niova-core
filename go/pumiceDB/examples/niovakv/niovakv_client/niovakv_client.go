package main

import (
	"flag"
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"

	"niovakv/httpclient"
	"niovakv/niovakvlib"
	"niovakv/serfclienthandler"
)

var (
	ClientHandler                                            serfclienthandler.SerfClientHandler
	config_path, operation, key, value, logPath, logFilename string
	retries                                                  int
)

//Create logfile for client.
func initLogger() {

	var filename string = logPath + "/" + logFilename + ".log"

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
	flag.StringVar(&logPath, "l", "/tmp", "log filepath")
	flag.StringVar(&logFilename, "f", "niovakvClient", "log filename")
	flag.StringVar(&operation, "o", "NULL", "write/read operation")
	flag.StringVar(&key, "k", "NULL", "Key")
	flag.StringVar(&value, "v", "NULL", "Value")
	flag.Parse()
}

//Get any client addr
func getServerAddr(refresh bool) (string, string) {
	if refresh {
		ClientHandler.GetData(false)
	}
	//Get random addr
	if len(ClientHandler.Agents) <= 0 {
		log.Error("All servers are dead")
		os.Exit(1)
	}
	//randomIndex := rand.Intn(len(ClientHandler.Agents))
	randomNode := ClientHandler.Agents[retries%len(ClientHandler.Agents)]
	retries += 1
	return ClientHandler.AgentData[randomNode].Addr, ClientHandler.AgentData[randomNode].Tags["Hport"]
}

func main() {

	//Get commandline parameters.
	getCmdParams()

	//Create log file.
	initLogger()

	//For serf client init
	ClientHandler = serfclienthandler.SerfClientHandler{}
	ClientHandler.Retries = 5
	ClientHandler.AgentData = make(map[string]*serfclienthandler.Data)
	ClientHandler.Initdata(config_path)
	var reqObj niovakvlib.NiovaKV
	var doOperation func(*niovakvlib.NiovaKV, string, string) error
	if operation == "write" {
		reqObj.InputOps = operation
		reqObj.InputKey = key
		reqObj.InputValue = []byte(value)
		doOperation = httpclient.WriteRequest
	} else {
		reqObj.InputOps = operation
		reqObj.InputKey = key
		doOperation = httpclient.ReadRequest
	}

	//Do upto 5 times if request failed
	addr, port := getServerAddr(true)
	for j := 0; j < 2; j++ {
		fmt.Println("HTTP addr : ", addr, port)
		err := doOperation(&reqObj, addr, port)
		fmt.Println(err)
		if err == nil {
			break
		}
		addr, port = getServerAddr(false)
		log.Error(err)
	}
}
