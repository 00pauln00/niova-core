package serviceDiscovery

import (
	"common/httpClient"
	"common/serfClient"
	"encoding/json"
	"errors"
	"io/ioutil"
	"math/rand"
	"sync"
	"time"
	PumiceDBCommon "niova/go-pumicedb-lib/common"
	compressionLib "common/specificCompressionLib"
	log "github.com/sirupsen/logrus"

	client "github.com/hashicorp/serf/client"
)


type ServiceDiscoveryHandler struct {
	//Exported
	HTTPRetry             int //No of seconds for a request time out and membership table refresh
	SerfRetry	      int
	ServerChooseAlgorithm int
	UseSpecificServerName string

	//Stat
	RequestDistribution map[string]*ServerRequestStat
	RequestSentCount    int64
	RequestSuccessCount int64
	RequestFailedCount  int64
	IsStatRequired      bool

	//UnExported
	servers           []client.Member
	serfClientObj     serfClient.SerfClientHandler
	serfUpdateLock    sync.Mutex
	agentTableLock    sync.Mutex
	statUpdateLock    sync.Mutex
	ready             bool
	specificServer    *client.Member
	roundRobinPtr     int
}

type ServerRequestStat struct {
	Count   int64
	Success int64
	Failed  int64
}

func (handler *ServerRequestStat) updateStat(ok bool) {
	handler.Count += int64(1)
	if ok {
		handler.Success += int64(1)
	} else {
		handler.Failed += int64(1)
	}
}

func (handler *ServiceDiscoveryHandler) dumpIntoJson(outfilepath string) {

	//prepare path for temporary json file.
	tempOutfileName := outfilepath + "/" + "reqdistribution" + ".json"
	file, _ := json.MarshalIndent(handler, "", "\t")
	_ = ioutil.WriteFile(tempOutfileName, file, 0644)

}

func getAddr(member *client.Member) (string, string) {
	return member.Addr.String(), member.Tags["Hport"]
}

func (handler *ServiceDiscoveryHandler) Request(payload []byte, suburl string, write bool) ([]byte, error) {
	var toSend client.Member
	var response []byte
	var err error

	for i := 0; i < handler.HTTPRetry; i++ {
		var ok bool

		//Get node to send request to
		toSend, err = handler.pickServer(toSend.Name)
		if err != nil {
			log.Error("Error while choosing node : ", err)
			break
		}

		//Get node's http address
		addr, port := getAddr(&toSend)
		response, err = httpClient.HTTP_Request(payload, addr+":"+port+suburl, write)
		if err == nil {
			ok = true
		}

		//Update request stat
		if handler.IsStatRequired {
			handler.statUpdateLock.Lock()
			if _, present := handler.RequestDistribution[toSend.Name]; !present {
				handler.RequestDistribution[toSend.Name] = &ServerRequestStat{}
			}
			handler.RequestDistribution[toSend.Name].updateStat(ok)
			handler.statUpdateLock.Unlock()
		}

		//Break from retry loop
		if ok {
			break
		}

		log.Error("Error in HTTP request : ", err)
		log.Trace("Retrying HTTP request with a different proxy")
		time.Sleep(1 * time.Second)
	}

	if handler.IsStatRequired {
		handler.RequestSentCount += int64(1)
		if response != nil {
			handler.RequestSuccessCount += int64(1)
		} else {
			handler.RequestFailedCount += int64(1)
		}
	}

	return response, err
}

func isValidNodeData(member client.Member) bool {
	if ((member.Status != "alive") || (member.Tags["Hport"] == "") || (member.Tags["Type"] != "PROXY")) {
		return false
	}
	return true
}

func (handler *ServiceDiscoveryHandler) pickServer(removeName string) (client.Member, error) {
	handler.agentTableLock.Lock()
	defer handler.agentTableLock.Unlock()
	var serverChoosen *client.Member
	switch handler.ServerChooseAlgorithm {
	case 0:
		//Random proxy chooser
		var randomIndex int
		for {
			if len(handler.servers) == 0 {
				return client.Member{}, errors.New("(Service discovery) Server not available")
			}
			randomIndex = rand.Intn(len(handler.servers))
			if removeName != "" {
				log.Trace(removeName)
			}

			//Check if node is alive, check if gossip is available and http server of that node is not reported down!
			if (isValidNodeData(handler.servers[randomIndex])) && (removeName != handler.servers[randomIndex].Name) {
				break
			}
			handler.servers = removeIndex(handler.servers, randomIndex)
		}
		serverChoosen = &handler.servers[randomIndex]

	case 1:
		//Round-Robin based proxy chooser
		for {
			if len(handler.servers) == 0 {
                                return client.Member{}, errors.New("(Service discovery) Server not available")
                        }
			handler.roundRobinPtr %= len(handler.servers)
			serverChoosen = &handler.servers[handler.roundRobinPtr]
			if (isValidNodeData(handler.servers[handler.roundRobinPtr])) {
				handler.roundRobinPtr += 1
				break
			}
			handler.servers = removeIndex(handler.servers, handler.roundRobinPtr)
			handler.roundRobinPtr += 1
		}

	case 2:
		//Specific node chooser
		if handler.UseSpecificServerName == removeName {
			log.Error("(Service discovery) Unable to connect with specified server")
			return client.Member{}, errors.New("Unable to connect with specified server")
		}
		if handler.specificServer != nil {
			serverChoosen = handler.specificServer
			break
		}
		for _, member := range handler.servers {
			if member.Name == handler.UseSpecificServerName {
				serverChoosen = &member
				break
			}
		}
	}
	return *serverChoosen, nil
}

func (handler *ServiceDiscoveryHandler) initSerfClient(configPath string) error {
	handler.serfClientObj.Retries = handler.SerfRetry
	return handler.serfClientObj.InitData(configPath)
}

func (handler *ServiceDiscoveryHandler) memberSearcher(stop chan int) error {
comparison:
	for {
		select {
		case <-stop:
			log.Info("stopping member updater")
			break comparison
		default:
			//Get latest membership data
			handler.serfUpdateLock.Lock()
			err := handler.serfClientObj.UpdateSerfClient(true)
			handler.serfUpdateLock.Unlock()
			if err != nil {
				return err
			}

			//Update local agent table
			handler.agentTableLock.Lock()
			handler.servers = handler.serfClientObj.Agents
			handler.agentTableLock.Unlock()
			handler.ready = true
			time.Sleep(1 * time.Second)
		}
	}

	return nil
}

func (handler *ServiceDiscoveryHandler) StartClientAPI(stop chan int, configPath string) error {
	var err error
	handler.RequestDistribution = make(map[string]*ServerRequestStat)

	//Connect with serf agent to get mesh details
	err = handler.initSerfClient(configPath)
	if err != nil {
		return err
	}

	//Search mesh for proxy to handle KV request
	err = handler.memberSearcher(stop)
	if err != nil {
		return err
	}
	return err
}

func removeIndex(s []client.Member, index int) []client.Member {
	ret := make([]client.Member, 0)
	ret = append(ret, s[:index]...)
	return append(ret, s[index+1:]...)
}

func (handler *ServiceDiscoveryHandler) getConfig(configPath string) error {
	handler.serfClientObj.Retries = 5
	return handler.serfClientObj.InitData(configPath)
}

func (handler *ServiceDiscoveryHandler) GetMembership() map[string]client.Member {
	handler.serfUpdateLock.Lock()
	defer handler.serfUpdateLock.Unlock()
	return handler.serfClientObj.GetMemberList()
}


func getAnyEntryFromStringMap(mapSample map[string]map[string]string) map[string]string {
        for _,v := range mapSample {
                return v
        }
        return nil
}

func (handler *ServiceDiscoveryHandler) GetPMDBServerConfig() ([]byte, error) {
	PMDBServerConfigMap := make(map[string]PumiceDBCommon.PeerConfigData)
	allPmdbServerGossip := handler.serfClientObj.GetTags("Type","PMDB_SERVER")
	pmdbServerGossip := getAnyEntryFromStringMap(allPmdbServerGossip)

	for key, value := range pmdbServerGossip {
		uuid, err := compressionLib.DecompressUUID(key)
                if err == nil {
			peerConfig := PumiceDBCommon.PeerConfigData{}
			compressionLib.DecompressStructure(&peerConfig,key+value)
			PMDBServerConfigMap[uuid] = peerConfig
		}
	}
	return json.MarshalIndent(PMDBServerConfigMap, " ", "")
}

//Returns raft leader's uuid
func (handler *ServiceDiscoveryHandler) GetLeader() string {
	agent, err := handler.pickServer("")
	if err != nil {
		return "Servers unreachable"
	}
	return agent.Tags["Leader UUID"]
}

//Wait till connect
func (handler *ServiceDiscoveryHandler) TillReady(service string) {
	//Check the client
	for !handler.ready {
		
	}
	//Check the service
	_, err := handler.pickServer("")
	for err != nil {
	    	_, err = handler.pickServer("")
		time.Sleep(5)
	}
}
