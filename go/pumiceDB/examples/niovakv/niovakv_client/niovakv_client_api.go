package clientapi

import (
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"

	"niovakv/httpclient"
	"niovakv/niovakvlib"

	"niovakv/serfclienthandler"
)

type NiovakvClient struct {
	addies         []addrStruct
	ClientHandler  serfclienthandler.SerfClientHandler
	addrTableLock  sync.Mutex
	serfUpdateLock sync.Mutex
}

type addrStruct struct {
	addr   string
	failed *int32
}

func (nkvc *NiovakvClient) Put(ReqObj *niovakvlib.NiovaKV) int {
	ReqObj.InputOps = "write"
	// nkvc.SerfClientInit()
	toSendAddr := nkvc.pickServer()
	//Do upto 5 times if request failed
	var responseRecvd *niovakvlib.NiovaKVResponse
	for j := 0; j < 5; j++ {
		var err error
		responseRecvd, err = httpclient.PutRequest(ReqObj, toSendAddr.addr)
		if err == nil {
			break
		}
		log.Error(err)
		atomic.AddInt32(toSendAddr.failed, 1)
		toSendAddr = nkvc.pickServer()
	}
	return responseRecvd.RespStatus
}

func (nkvc *NiovakvClient) Get(ReqObj *niovakvlib.NiovaKV) []byte {
	ReqObj.InputOps = "read"
	// nkvc.SerfClientInit()
	toSendAddr := nkvc.pickServer()
	//Do upto 5 times if request failed
	var responseRecvd *niovakvlib.NiovaKVResponse
	for j := 0; j < 5; j++ {
		var err error
		responseRecvd, err = httpclient.GetRequest(ReqObj, toSendAddr.addr)
		if err == nil {
			break
		}
		log.Error(err)
		atomic.AddInt32(toSendAddr.failed, 1)
		toSendAddr = nkvc.pickServer()
	}
	return responseRecvd.RespValue
}

func (nkvc *NiovakvClient) SerfClientInit() {
	nkvc.ClientHandler.Retries = 5
	nkvc.ClientHandler.Initdata("../config")
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
			err := nkvc.ClientHandler.GetData(true)
			nkvc.serfUpdateLock.Unlock()
			if err != nil {
				log.Error("Unable to connect with agents")
				os.Exit(1)
			}
			var addies []addrStruct
			//Only alive addr will be there in Agents
			for _, mems := range nkvc.ClientHandler.Agents {
				a := int32(0)
				addies = append(addies, addrStruct{
					addr:   mems.Addr.String() + ":" + mems.Tags["Hport"],
					failed: &a,
				})
			}
			nkvc.addrTableLock.Lock()
			nkvc.addies = addies
			nkvc.addrTableLock.Unlock()
			time.Sleep(2 * time.Second)
		}
	}
}

func (nkvc *NiovakvClient) Start(stop chan int) {
	nkvc.SerfClientInit()
	go nkvc.MemberSearcher(stop)
}

func (nkvc *NiovakvClient) pickServer() *addrStruct {
	nkvc.addrTableLock.Lock()
	defer nkvc.addrTableLock.Unlock()

	if len(nkvc.addies) == 0 {
		log.Error("no alive servers")
		os.Exit(1)
	}

	//Get random addr and delete if its failed and provide with non-failed one!
	var randomIndex int
	for {
		randomIndex = rand.Intn(len(nkvc.addies))
		if atomic.LoadInt32(nkvc.addies[randomIndex].failed) == 0 {
			break
		}
		nkvc.addies = removeIndex(nkvc.addies, randomIndex)
	}

	return &nkvc.addies[randomIndex]
}

//Returns raft leader's uuid
func (nkvc *NiovakvClient) GetLeader() string {
	nkvc.serfUpdateLock.Lock()
	defer nkvc.serfUpdateLock.Unlock()
	return nkvc.ClientHandler.Agents[0].Tags["Leader UUID"]
}

func removeIndex(s []addrStruct, index int) []addrStruct {
	ret := make([]addrStruct, 0)
	ret = append(ret, s[:index]...)
	return append(ret, s[index+1:]...)
}
