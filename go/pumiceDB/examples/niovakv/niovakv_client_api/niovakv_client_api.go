package clientapi

import (
	"errors"
	"math/rand"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"niovakv/httpclient"
	"niovakv/niovakvlib"
	"niovakv/serfclienthandler"

	client "github.com/hashicorp/serf/client"
)

type NiovakvClient struct {
	//Exported
	Timeout time.Duration //No of seconds for a request time out and membership table refresh
	//UnExported
	servers        []client.Member
	clientHandler  serfclienthandler.SerfClientHandler
	serfUpdateLock sync.Mutex
	tableLock      sync.Mutex
}

func getAddr(member *client.Member) (string, string) {
	return member.Addr.String(), member.Tags["Hport"]
}

func (nkvc *NiovakvClient) doOperation(ReqObj *niovakvlib.NiovaKV, write bool) *niovakvlib.NiovaKVResponse {
	var responseRecvd *niovakvlib.NiovaKVResponse
	timer := time.Tick(nkvc.Timeout)
time:
	for {
		select {
		case <-timer:
			log.Error("Request timed at client side")
			break time
		default:
			var err error
			toSend, err := nkvc.pickServer()
			if err != nil {
				return nil
			}
			addr, port := getAddr(&toSend)
			if write {
				responseRecvd, err = httpclient.PutRequest(ReqObj, addr, port)
			} else {
				responseRecvd, err = httpclient.GetRequest(ReqObj, addr, port)
			}
			if err == nil {
				break time
			}
			log.Error(err)
		}
	}
	return responseRecvd
}

func (nkvc *NiovakvClient) Put(ReqObj *niovakvlib.NiovaKV) (int, []byte) {
	ReqObj.InputOps = "write"
	responseRecvd := nkvc.doOperation(ReqObj, true)
	return responseRecvd.RespStatus, responseRecvd.RespValue
}

func (nkvc *NiovakvClient) Get(ReqObj *niovakvlib.NiovaKV) (int, []byte) {
	ReqObj.InputOps = "read"
	responseRecvd := nkvc.doOperation(ReqObj, false)
	return responseRecvd.RespStatus, responseRecvd.RespValue
}

func (nkvc *NiovakvClient) serfClientInit(configPath string) error {
	nkvc.clientHandler.Retries = 5
	return nkvc.clientHandler.Initdata(configPath)
}

func (nkvc *NiovakvClient) memberSearcher(stop chan int) error {
comparison:
	for {
		select {
		case <-stop:
			log.Info("stopping member updater")
			break comparison
		default:
			//Since we do update it continuesly, we persist the connection
			nkvc.serfUpdateLock.Lock()
			err := nkvc.clientHandler.GetData(true)
			nkvc.serfUpdateLock.Unlock()
			if err != nil {
				log.Error("Unable to connect with agents")
				return err
			}
			nkvc.tableLock.Lock()
			nkvc.servers = nkvc.clientHandler.Agents
			nkvc.tableLock.Unlock()
			time.Sleep(nkvc.Timeout)
		}
	}
	return nil
}

func (nkvc *NiovakvClient) Start(stop chan int, configPath string) error {
	var err error
	err = nkvc.serfClientInit(configPath)
	if err != nil {
		log.Error("Error while initializing the serf client ", err)
		return err
	}
	err = nkvc.memberSearcher(stop)
	if err != nil {
		log.Error("Error while starting the membership updater ", err)
		return err
	}
	return err
}

func (nkvc *NiovakvClient) pickServer() (client.Member, error) {
	nkvc.tableLock.Lock()
	defer nkvc.tableLock.Unlock()
	//Get random addr and delete if its failed and provide with non-failed one!
	var randomIndex int
	for {
		if len(nkvc.servers) == 0 {
			log.Error("no alive servers")
			return client.Member{}, errors.New("No alive servers")
		}
		randomIndex = rand.Intn(len(nkvc.servers))
		if nkvc.servers[randomIndex].Status == "alive" {
			break
		}
		nkvc.servers = removeIndex(nkvc.servers, randomIndex)
	}

	return nkvc.servers[randomIndex], nil
}

//Returns raft leader's uuid
func (nkvc *NiovakvClient) GetLeader() string {
	agent, err := nkvc.pickServer()
	if err != nil {
		return "Servers unreachable"
	}
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
