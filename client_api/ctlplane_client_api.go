package client_api

import (
        "errors"
        "math/rand"
        "sync"
	"strings"
	"encoding/json"
	"io/ioutil"
        "time"
        log "github.com/sirupsen/logrus"
        "ctlplane/httpclient"
        "ctlplane/serfclienthandler"
        client "github.com/hashicorp/serf/client"
)

type ClientAPI struct {
	//Exported
	Timeout               time.Duration //No of seconds for a request time out and membership table refresh
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
	clientHandler     serfclienthandler.SerfClientHandler
	serfUpdateLock    sync.Mutex
	tableLock         sync.Mutex
	requestUpdateLock sync.Mutex
	ready             bool
	specificServer    *client.Member
	roundRobinPtr     int
}

type ServerRequestStat struct {
	Count   int64
	Success int64
	Failed  int64
}

func (stat *ServerRequestStat) updateStat(ok bool) {
	stat.Count += int64(1)
	if ok {
		stat.Success += int64(1)
	} else {
		stat.Failed += int64(1)
	}
}

func (ncpcClientAPIObj *ClientAPI) DumpIntoJson(outfilepath string){

        //prepare path for temporary json file.
        tempOutfileName := outfilepath+"/"+"reqdistribution"+".json"
        file, _ := json.MarshalIndent(ncpcClientAPIObj, "", "\t")
        _ = ioutil.WriteFile(tempOutfileName, file, 0644)

}


func getAddr(member *client.Member) (string, string) {
	return member.Addr.String(), member.Tags["Hport"]
}

func (ncpc *ClientAPI) Request(payload []byte, suburl string, write bool) []byte {
	var toSend client.Member
	var response []byte
	Qtimer := time.Tick(ncpc.Timeout * time.Second)
time:
	for {
		select {
		case <-Qtimer:
			log.Error("Request timed at client side")
			break time
		default:
			var err error
			var ok bool

			toSend, err = ncpc.pickServer(toSend.Name)
			if err != nil {
				break time
			}

			addr, port := getAddr(&toSend)
			response, err = httpclient.Request(payload, addr+":"+port+suburl, write)
			if err == nil {
				ok = true
			}

			if ncpc.IsStatRequired {
				ncpc.requestUpdateLock.Lock()
				if _, present := ncpc.RequestDistribution[toSend.Name]; !present {
					ncpc.RequestDistribution[toSend.Name] = &ServerRequestStat{}
				}
				ncpc.RequestDistribution[toSend.Name].updateStat(ok)
				ncpc.requestUpdateLock.Unlock()
			}

			if ok {
				break time
			}
			log.Error(err)
		}
	}

	if ncpc.IsStatRequired {
		ncpc.RequestSentCount += int64(1)
		if response != nil {
			ncpc.RequestSuccessCount += int64(1)
		} else {
			ncpc.RequestFailedCount += int64(1)
		}
	}

	return response
}

func isValidNodeData(member client.Member) bool {
	if (member.Status != "alive") || (member.Tags["Hport"] == "") || (member.Tags["Type"] == "PMDB_SERVER") {
		return false
	}
	return true
}

func (ncpc *ClientAPI) pickServer(removeName string) (client.Member, error) {
	ncpc.tableLock.Lock()
	defer ncpc.tableLock.Unlock()
	var serverChoosen *client.Member
	switch ncpc.ServerChooseAlgorithm {
	case 0:
		//Random
		var randomIndex int
		for {
			if len(ncpc.servers) == 0 {
				log.Error("(CLIENT API MODULE) no alive servers")
				return client.Member{}, errors.New("No alive servers")
			}
			randomIndex = rand.Intn(len(ncpc.servers))
			if removeName != "" {
				log.Info(removeName)
			}

			//Check if node is alive, check if gossip is available and http server of that node is not reported down!
			if (isValidNodeData(ncpc.servers[randomIndex])) && (removeName != ncpc.servers[randomIndex].Name) {
				break
			}
			ncpc.servers = removeIndex(ncpc.servers, randomIndex)
		}

		serverChoosen = &ncpc.servers[randomIndex]
	case 1:
		//Round-Robin
		ncpc.roundRobinPtr %= len(ncpc.servers)
		serverChoosen = &ncpc.servers[ncpc.roundRobinPtr]
		ncpc.roundRobinPtr += 1
	case 2:
		//Specific
		if ncpc.specificServer != nil {
			serverChoosen = ncpc.specificServer
			break
		}
		for _, member := range ncpc.servers {
			if member.Name == ncpc.UseSpecificServerName {
				serverChoosen = &member
				break
			}
		}
	}
	return *serverChoosen, nil
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
			time.Sleep(1 * time.Second)
		}
	}

	return nil
}

func (nkvc *ClientAPI) Start(stop chan int, configPath string) error {
	var err error
	nkvc.RequestDistribution = make(map[string]*ServerRequestStat)
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

func removeIndex(s []client.Member, index int) []client.Member {
	ret := make([]client.Member, 0)
	ret = append(ret, s[:index]...)
	return append(ret, s[index+1:]...)
}

func (nkvc *ClientAPI) GetConfig(configPath string) error {
	nkvc.clientHandler.Retries = 5
	return nkvc.clientHandler.Initdata(configPath)
}

func (nkvc *ClientAPI) GetPMDBServerConfig() ([]byte,error){
	type PeerConfigData struct{
		PeerUUID   string
		IPAddr     string
		Port       string
		ClientPort string
	}
        var PeerUUID, ClientPort, Port, IPAddr string
	PMDBServerConfigMap := make(map[string]PeerConfigData)

	allConfig := nkvc.clientHandler.GetPMDBConfig()
	splitData := strings.Split(allConfig, "/")
	flag := false
        for i, element := range splitData {
                switch i % 4 {
                case 0:
                        PeerUUID = element
                case 1:
                        IPAddr = element
                case 2:
                        ClientPort = element
                case 3:
                        flag = true
                        Port = element
                }
                if flag {
                        peerConfig := PeerConfigData{
                                PeerUUID:   PeerUUID,
                                IPAddr:     IPAddr,
                                Port:       Port,
                                ClientPort: ClientPort,
                        }
                        PMDBServerConfigMap[PeerUUID] = peerConfig
                        flag = false
                }
        }

	return json.MarshalIndent(PMDBServerConfigMap," ","")
}

func (nkvc *ClientAPI) Till_ready() {
	for !nkvc.ready {

	}
}
