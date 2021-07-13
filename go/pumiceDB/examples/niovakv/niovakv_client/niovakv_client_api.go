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
<<<<<<< HEAD
<<<<<<< HEAD

		if err == nil {
			break
		}
		log.Error(err)
		nkvc.Addr, nkvc.Port, err = GetServerAddr(false, "./config")
		if err != nil {
			log.Error(err)
			os.Exit(1)
		}
=======
=======

>>>>>>> rebased and getting niovakv_client to work with api again
		if err == nil {
			break
		}
		log.Error(err)
<<<<<<< HEAD
>>>>>>> changed layout of niovakv_client directory
=======
		nkvc.Addr, nkvc.Port, err = GetServerAddr(false, "./config")
		if err != nil {
			log.Error(err)
			os.Exit(1)
		}
>>>>>>> rebased and getting niovakv_client to work with api again
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
<<<<<<< HEAD
<<<<<<< HEAD

		if err == nil {
			break
		}
		log.Error(err)
		nkvc.Addr, nkvc.Port, err = GetServerAddr(false, "./config")
		if err != nil {
			log.Error(err)
			os.Exit(1)
		}
=======
=======

>>>>>>> rebased and getting niovakv_client to work with api again
		if err == nil {
			break
		}
		log.Error(err)
<<<<<<< HEAD
>>>>>>> changed layout of niovakv_client directory
=======
		nkvc.Addr, nkvc.Port, err = GetServerAddr(false, "./config")
		if err != nil {
			log.Error(err)
			os.Exit(1)
		}
>>>>>>> rebased and getting niovakv_client to work with api again
	}
	return responseRecvd.RespValue
}

<<<<<<< HEAD
<<<<<<< HEAD
func GetServerAddr(refresh bool, config_path string) (string, string, error) {
	ClientHandler := serfclienthandler.SerfClientHandler{}
	ClientHandler.Retries = 5
	ClientHandler.AgentData = make(map[string]*serfclienthandler.Data)
	ClientHandler.Initdata(config_path)
	retries := 5
	var err error
=======
func GetServerAddr(refresh bool) (string, string) {
=======
func GetServerAddr(refresh bool, config_path string) (string, string, error) {
>>>>>>> rebased and getting niovakv_client to work with api again
	ClientHandler := serfclienthandler.SerfClientHandler{}
	ClientHandler.Retries = 5
	ClientHandler.AgentData = make(map[string]*serfclienthandler.Data)
	ClientHandler.Initdata(config_path)
	retries := 5
<<<<<<< HEAD
>>>>>>> changed layout of niovakv_client directory
=======
	var err error
>>>>>>> rebased and getting niovakv_client to work with api again
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
<<<<<<< HEAD
<<<<<<< HEAD
	return ClientHandler.AgentData[randomNode].Addr, ClientHandler.AgentData[randomNode].Tags["Hport"], err
=======
	return ClientHandler.AgentData[randomNode].Addr, ClientHandler.AgentData[randomNode].Tags["Hport"]
>>>>>>> changed layout of niovakv_client directory
=======
	return ClientHandler.AgentData[randomNode].Addr, ClientHandler.AgentData[randomNode].Tags["Hport"], err
>>>>>>> rebased and getting niovakv_client to work with api again
}
