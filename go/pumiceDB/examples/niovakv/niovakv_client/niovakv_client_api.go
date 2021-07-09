package clientapi

import (
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
		responseRecvd, err = httpclient.WriteRequest(nkvc.ReqObj, nkvc.Addr, nkvc.Port)
		if err == nil {
			break
		}
		nkvc.Addr, nkvc.Port = GetServerAddr(true)
		log.Error(err)
	}
	return responseRecvd.RespStatus
}

func (nkvc *NiovakvClient) Get() []byte {
	nkvc.ReqObj.InputOps = "read"
	//Do upto 5 times if request failed
	var responseRecvd *niovakvlib.NiovaKVResponse
	for j := 0; j < 5; j++ {
		var err error
		responseRecvd, err = httpclient.ReadRequest(nkvc.ReqObj, nkvc.Addr, nkvc.Port)
		if err == nil {
			break
		}
		nkvc.Addr, nkvc.Port = GetServerAddr(true)
		log.Error(err)
	}
	return responseRecvd.RespValue
}

func GetServerAddr(refresh bool) (string, string) {
	ClientHandler := serfclienthandler.SerfClientHandler{}
	retries := 5
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
