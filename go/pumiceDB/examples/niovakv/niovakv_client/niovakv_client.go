package main

import (
	"flag"
	"math/rand"
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
	flag.StringVar(&config_path, "c", "./config", "config file path")
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
	//For serf client init
	getCmdParams()
	ClientHandler = serfclienthandler.SerfClientHandler{}
	ClientHandler.Initdata(config_path)
	ClientHandler.Retries = 5
	ClientHandler.AgentData = make(map[string]*serfclienthandler.Data)

	//Prepare the request to send on http
	reqObj := niovakvlib.NiovaKV{
		InputOps:   operation,
		InputKey:   key,
		InputValue: []byte(value),
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
