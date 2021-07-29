package clientapi

import (
	"math/rand"
	"os"
	"time"

	log "github.com/sirupsen/logrus"

	"niovakv/httpclient"
	"niovakv/niovakvlib"

	"niovakv/serfclienthandler"
)

type NiovakvClient struct {
	ReqObj        *niovakvlib.NiovaKV
	addr          string
	port          string
	addies        address
	ClientHandler serfclienthandler.SerfClientHandler
}

type address struct {
	addr []string
	port []string
}

func (nkvc *NiovakvClient) Put() int {
	nkvc.ReqObj.InputOps = "write"
	// nkvc.SerfClientInit()
	nkvc.pickServer()
	//Do upto 5 times if request failed
	var responseRecvd *niovakvlib.NiovaKVResponse
	for j := 0; j < 5; j++ {
		var err error
		responseRecvd, err = httpclient.PutRequest(nkvc.ReqObj, nkvc.addr, nkvc.port)

		if err == nil {
			break
		}
		log.Error(err)
		nkvc.pickServer()
	}
	return responseRecvd.RespStatus
}

func (nkvc *NiovakvClient) Get() []byte {
	nkvc.ReqObj.InputOps = "read"
	// nkvc.SerfClientInit()
	nkvc.pickServer()
	//Do upto 5 times if request failed
	var responseRecvd *niovakvlib.NiovaKVResponse
	for j := 0; j < 5; j++ {
		var err error
		responseRecvd, err = httpclient.GetRequest(nkvc.ReqObj, nkvc.addr, nkvc.port)

		if err == nil {
			break
		}
		log.Error(err)
		nkvc.pickServer()
	}
	return responseRecvd.RespValue
}

func (nkvc *NiovakvClient) SerfClientInit() {
	nkvc.ClientHandler.Retries = 5
	nkvc.ClientHandler.AgentData = make(map[string]*serfclienthandler.Data)
	nkvc.ClientHandler.Initdata("../config")
}

func (nkvc *NiovakvClient) GetServerAddr(refresh bool) ([]string, []string) {
	//change this to get from list

	// var err error
	if refresh {
		nkvc.ClientHandler.GetData(false)
	}
	//Get random addr
	if len(nkvc.ClientHandler.Agents) <= 0 {
		log.Error("All servers are dead")
		os.Exit(1)
	}
	var addr []string
	var port []string
	members := nkvc.ClientHandler.MemberMapPtr
	for _, mems := range *members {
		addr = append(addr, mems.Addr.String())
		port = append(port, mems.Tags["Hport"])
	}
	log.Debug("this is the addr", addr)
	log.Debug("this is the port", port)
	return addr, port
}

func (nkvc *NiovakvClient) MemberUpdater(stop chan int) {
comparison:
	for {
		select {
		case <-stop:
			log.Info("stopping member updater")
			break comparison
		default:
			var newAddrs []string
			var newPorts []string
			newAddrs, newPorts = nkvc.GetServerAddr(true)
			for i, newPortI := range newPorts {
				var matchFound bool
				for g, basePortG := range nkvc.addies.port {
					var exists bool
					if newPortI == basePortG {
						matchFound = true
					}
					for _, newPortZ := range newPorts {

						if newPortZ == basePortG {
							exists = true
						}
					}
					if !exists {
						log.Info("a server is down")
						nkvc.addies.addr = removeIndex(nkvc.addies.addr, g)
						nkvc.addies.port = removeIndex(nkvc.addies.port, g)
					}
				}
				if !matchFound {
					nkvc.addies.addr = append(nkvc.addies.addr, newAddrs[i])
					nkvc.addies.port = append(nkvc.addies.port, newPortI)
				}
			}
		}
		time.Sleep(1 * time.Second)
	}
}

func (nkvc *NiovakvClient) Start(stop chan int) {
	nkvc.SerfClientInit()
	nkvc.addies.addr, nkvc.addies.port = nkvc.GetServerAddr(true)
	go nkvc.MemberUpdater(stop)
}

func (nkvc *NiovakvClient) pickServer() {
	randomIndex := rand.Intn(len(nkvc.addies.addr))
	nkvc.addr = nkvc.addies.addr[randomIndex]
	nkvc.port = nkvc.addies.port[randomIndex]
}

func removeIndex(s []string, index int) []string {
	ret := make([]string, 0)
	ret = append(ret, s[:index]...)
	return append(ret, s[index+1:]...)
}
