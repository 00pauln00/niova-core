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

type ClientAPI struct {
	//Exported
	Timeout time.Duration //No of seconds for a request time out and membership table refresh

	//Stat
        RequestDistribution   map[string]ServerRequestStat
        RequestSentCount      int64
        RequestSuccessCount   int64
        RequestFailedCount    int64
        IsStatRequired        bool

	//UnExported
	servers           []client.Member
	clientHandler     serfclienthandler.SerfClientHandler
	serfUpdateLock    sync.Mutex
	tableLock         sync.Mutex
	requestUpdateLock sync.Mutex
	ready             bool
}

type ServerRequestStat struct{
        Count   int64
        Success int64
        Failed  int64
}

func (stat ServerRequestStat) updateStat(ok bool) {
        stat.Count += int64(1)
        if ok{
                stat.Success += int64(1)
        } else{
                stat.Failed += int64(1)
        }
}


func getAddr(member *client.Member) (string, string) {
	return member.Addr.String(), member.Tags["Hport"]
}

func (nkvc *ClientAPI) doOperation(ReqObj *niovakvlib.NiovaKV, write bool) *niovakvlib.NiovaKVResponse {
	var responseRecvd *niovakvlib.NiovaKVResponse
	timer := time.Tick(nkvc.Timeout * time.Second)
time:
	for {
		select {
		case <-timer:
			log.Error("Request timed at client side")
			break time
		default:
			var err error
			var ok bool

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
                                ok = true
                        }

                        if nkvc.IsStatRequired {
                                nkvc.requestUpdateLock.Lock()
                                if _,present := nkvc.RequestDistribution[toSend.Name]; !present{
                                        nkvc.RequestDistribution[toSend.Name] = ServerRequestStat{}
                                }
                                nkvc.RequestDistribution[toSend.Name].updateStat(ok)
                                nkvc.requestUpdateLock.Unlock()
                        }

			if ok {
				break time
			}
			log.Error(err)
		}
	}

	if nkvc.IsStatRequired {
                nkvc.RequestSentCount += int64(1)
                if responseRecvd!=nil{
                        nkvc.RequestSuccessCount += int64(1)
                } else{
                        nkvc.RequestFailedCount += int64(1)
                }
        }

	return responseRecvd
}

func (nkvc *ClientAPI) Put(ReqObj *niovakvlib.NiovaKV) (int, []byte) {
	ReqObj.InputOps = "write"
	response := nkvc.doOperation(ReqObj, true)
	if response == nil {
		return -1, nil
	}
	return response.RespStatus, response.RespValue
}

func (nkvc *ClientAPI) Get(ReqObj *niovakvlib.NiovaKV) (int, []byte) {
	ReqObj.InputOps = "read"
	response := nkvc.doOperation(ReqObj, false)
	if response == nil {
                return -1, nil
        }
	return response.RespStatus, response.RespValue
}

func (nkvc *ClientAPI) serfClientInit(configPath string) error {
	nkvc.clientHandler.Retries = 5
	return nkvc.clientHandler.Initdata(configPath)
}

func (nkvc *ClientAPI) memberSearcher(stop chan int) error {
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
			nkvc.ready = true
			time.Sleep(nkvc.Timeout)
		}
	}

	return nil
}

func (nkvc *ClientAPI) Start(stop chan int, configPath string) error {
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


func isGossipAvailable(member client.Member) bool{
	if member.Tags["Hport"] == "" {
		return false
	}
	return true
}


func (nkvc *ClientAPI) pickServer() (client.Member, error) {
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
		if ((nkvc.servers[randomIndex].Status == "alive") && (isGossipAvailable(nkvc.servers[randomIndex]))) {
			break
		}
		nkvc.servers = removeIndex(nkvc.servers, randomIndex)
	}

	return nkvc.servers[randomIndex], nil
}


//Returns raft leader's uuid
func (nkvc *ClientAPI) GetLeader() string {
	agent, err := nkvc.pickServer()
	if err != nil {
		return "Servers unreachable"
	}
	return agent.Tags["Leader UUID"]
}

func (nkvc *ClientAPI) GetMembership() map[string]client.Member {
	nkvc.serfUpdateLock.Lock()
	defer nkvc.serfUpdateLock.Unlock()
	return nkvc.clientHandler.GetMemberListMap()
}

func (nkvc *ClientAPI) Tillready() {
	for !nkvc.ready {

	}
}

func removeIndex(s []client.Member, index int) []client.Member {
	ret := make([]client.Member, 0)
	ret = append(ret, s[:index]...)
	return append(ret, s[index+1:]...)
}
