package main

import (
	"flag"
	"niovakv/httpclient"
	"niovakv/niovakvlib"
	"niovakv/serfclienthandler"
)

var ClientHandler serfclienthandler.SerfClientHandler
var config_path string
var operation string
var key string
var value string

//Function to get command line parameters while starting of the client.
func getCmdParams() {
	flag.StringVar(&config_path, "c", "./", "config file path")
	flag.StringVar(&operation, "o", "NULL", "write/read operation")
	flag.StringVar(&key, "k", "NULL", "Key")
	flag.StringVar(&value, "v", "NULL", "Value")
}

//Get any client addr
func getServerAddr(refresh bool) (string, string) {
	if refresh {
		ClientHandler.GetData(true)
	}
	for _, random := range ClientHandler.AgentAddrs {
		return random, ClientHandler.Hport
	}
	return "", ""
}

func main() {
	//For serf client init
	var filename string //Config file name
	ClientHandler = serfclienthandler.SerfClientHandler{}
	ClientHandler.Initdata(filename)
	ClientHandler.Retries = 5
	ClientHandler.AgentData = make(map[string]*serfclienthandler.Data)

	//Prepare the request to send on http
	reqObj := niovakvlib.NiovaKV{
		InputOps:   operation,
		InputKey:   key,
		InputValue: []byte(value),
	}

	// Get the alive http server IP and port
	addr, port := getServerAddr(true)

	//Send the request over http
	httpclient.SendRequest(&reqObj, addr, port)
}
