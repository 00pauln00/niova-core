package clientapi

import (
	"math/rand"
	"os"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"niovakv/httpclient"
	"niovakv/niovakvlib"
	"niovakv/serfclienthandler"

	client "github.com/hashicorp/serf/client"
)

type NiovakvClient struct {
	servers       []client.Member
	clientHandler serfclienthandler.SerfClientHandler
	//httpStatus     map[string]bool
	serfUpdateLock sync.Mutex
	tableLock      sync.Mutex
}

func getAddr(member *client.Member) string {
	httpAddr := member.Addr.String() + ":" + member.Tags["Hport"]
	return httpAddr
}

func (nkvc *NiovakvClient) Put(ReqObj *niovakvlib.NiovaKV) int {
	ReqObj.InputOps = "write"
	// nkvc.SerfClientInit()
	toSend := nkvc.pickServer()
	//Do upto 5 times if request failed
	var responseRecvd *niovakvlib.NiovaKVResponse
	for j := 0; j < 5; j++ {
		var err error
		addr := getAddr(&toSend)
		responseRecvd, err = httpclient.PutRequest(ReqObj, addr)
		if err == nil {
			break
		}
		log.Error(err)
		toSend = nkvc.pickServer()
	}
	return responseRecvd.RespStatus
}

func (nkvc *NiovakvClient) Get(ReqObj *niovakvlib.NiovaKV) []byte {
	ReqObj.InputOps = "read"
	// nkvc.SerfClientInit()
	toSend := nkvc.pickServer()

	//Do upto 5 times if request failed
	var responseRecvd *niovakvlib.NiovaKVResponse
	for j := 0; j < 5; j++ {
		var err error
		addr := getAddr(&toSend)
		responseRecvd, err = httpclient.GetRequest(ReqObj, addr)
		if err == nil {
			break
		}
		log.Error(err)
		toSend = nkvc.pickServer()
	}
	return responseRecvd.RespValue
}

func (nkvc *NiovakvClient) SerfClientInit() {
	nkvc.clientHandler.Retries = 5
	nkvc.clientHandler.Initdata("../config")
}

func (nkvc *NiovakvClient) MemberSearcher(stop chan int) {
comparison:
	for {
		select {
		case <-stop:
			log.Info("stopping member updater")
			break comparison
		default:
			log.Info("Member table update initiated")
			//Since we do update it continuesly, we persist the connection
			nkvc.serfUpdateLock.Lock()
			err := nkvc.clientHandler.GetData(true)
			nkvc.serfUpdateLock.Unlock()
			if err != nil {
				log.Error("Unable to connect with agents")
				os.Exit(1)
			}
			nkvc.tableLock.Lock()
			nkvc.servers = nkvc.clientHandler.Agents
			nkvc.tableLock.Unlock()
			time.Sleep(2 * time.Second)
		}
	}
}

func (nkvc *NiovakvClient) Start(stop chan int) {
	nkvc.SerfClientInit()
	go nkvc.MemberSearcher(stop)
}

func (nkvc *NiovakvClient) pickServer() client.Member {
	nkvc.tableLock.Lock()
	defer nkvc.tableLock.Unlock()

	if len(nkvc.clientHandler.Agents) == 0 {
		log.Error("no alive servers")
		os.Exit(1)
	}

	//Get random addr and delete if its failed and provide with non-failed one!
	var randomIndex int
	for {
		randomIndex = rand.Intn(len(nkvc.servers))
		if nkvc.servers[randomIndex].Status == "alive" {
			break
		}
		nkvc.servers = removeIndex(nkvc.servers, randomIndex)
	}

	return nkvc.servers[randomIndex]
}

//Returns raft leader's uuid
func (nkvc *NiovakvClient) GetLeader() string {
	agent := nkvc.pickServer()
	return agent.Tags["Leader UUID"]
}

func (nkvc *NiovakvClient) GetMembership() map[string]client.Member {
	nkvc.serfUpdateLock.Lock()
	defer nkvc.serfUpdateLock.Unlock()
	return nkvc.clientHandler.GetMemberListMap()
}

func removeIndex(s []client.Member, index int) []client.Member {
	ret := make([]client.Member, 0)
	ret = append(ret, s[:index]...)
	return append(ret, s[index+1:]...)
}
