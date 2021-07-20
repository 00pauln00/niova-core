package clientapi

import (
	"math/rand"
	"os"

	log "github.com/sirupsen/logrus"

	"niovakv/httpclient"
	"niovakv/niovakvlib"

	"niovakv/serfclienthandler"
)

type NiovakvClient struct {
	ReqObj *niovakvlib.NiovaKV
	Addr   string
	Port   string
}

func (nkvc *NiovakvClient) Put() int {
	nkvc.ReqObj.InputOps = "write"
	//Do upto 5 times if request failed
	var responseRecvd *niovakvlib.NiovaKVResponse
	for j := 0; j < 5; j++ {
		var err error
		responseRecvd, err = httpclient.PutRequest(nkvc.ReqObj, nkvc.Addr, nkvc.Port)

		if err == nil {
			break
		}
		log.Error(err)
		nkvc.GetServerAddr(false, "./config")
	}
	return responseRecvd.RespStatus
}

func (nkvc *NiovakvClient) Get() []byte {
	nkvc.ReqObj.InputOps = "read"
	//Do upto 5 times if request failed
	var responseRecvd *niovakvlib.NiovaKVResponse
	for j := 0; j < 5; j++ {
		var err error
		responseRecvd, err = httpclient.GetRequest(nkvc.ReqObj, nkvc.Addr, nkvc.Port)

		if err == nil {
			break
		}
		log.Error(err)
		nkvc.GetServerAddr(false, "./config")
	}
	return responseRecvd.RespValue
}

func (nkvc *NiovakvClient) GetServerAddr(refresh bool, config_path string) /* (string, string, error)*/ {
	ClientHandler := serfclienthandler.SerfClientHandler{}
	ClientHandler.Retries = 5
	ClientHandler.AgentData = make(map[string]*serfclienthandler.Data)
	ClientHandler.Initdata(config_path)
	// var err error
	if refresh {
		ClientHandler.GetData(false)
	}
	//Get random addr
	if len(ClientHandler.Agents) <= 0 {
		log.Error("All servers are dead")
		os.Exit(1)
	}
	randomIndex := rand.Intn(len(ClientHandler.Agents))
	randomNode := ClientHandler.Agents[randomIndex]
	nkvc.Addr = ClientHandler.AgentData[randomNode].Addr
	nkvc.Port = ClientHandler.AgentData[randomNode].Tags["Hport"]
	// return err
}
