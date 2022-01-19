package clientAPI

import (
        "errors"
        "math/rand"
        "sync"
	"strings"
	"encoding/json"
	"io/ioutil"
        "time"
        log "github.com/sirupsen/logrus"
        "common/httpClient"
        "common/serfClient"

        client "github.com/hashicorp/serf/client"
)

type ClientAPIHandler struct {
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
	serfClientObj     serfClient.SerfClientHandler
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

func (handler *ServerRequestStat) update_Stat(ok bool) {
	handler.Count += int64(1)
	if ok {
		handler.Success += int64(1)
	} else {
		handler.Failed += int64(1)
	}
}

func (handler *ClientAPIHandler) Dump_Into_Json(outfilepath string){

        //prepare path for temporary json file.
        tempOutfileName := outfilepath+"/"+"reqdistribution"+".json"
        file, _ := json.MarshalIndent(handler, "", "\t")
        _ = ioutil.WriteFile(tempOutfileName, file, 0644)

}


func getAddr(member *client.Member) (string, string) {
	return member.Addr.String(), member.Tags["Hport"]
}

func (handler *ClientAPIHandler) Request(payload []byte, suburl string, write bool) []byte {
	var toSend client.Member
	var response []byte
	Qtimer := time.Tick(handler.Timeout * time.Second)
time:
	for {
		select {
		case <-Qtimer:
			log.Error("Request timed at client side")
			break time
		default:
			var err error
			var ok bool

			toSend, err = handler.pick_Server(toSend.Name)
			if err != nil {
				break time
			}

			addr, port := getAddr(&toSend)
			response, err = httpClient.HTTP_Request(payload, addr+":"+port+suburl, write)
			if err == nil {
				ok = true
			}

			if handler.IsStatRequired {
				handler.requestUpdateLock.Lock()
				if _, present := handler.RequestDistribution[toSend.Name]; !present {
					handler.RequestDistribution[toSend.Name] = &ServerRequestStat{}
				}
				handler.RequestDistribution[toSend.Name].update_Stat(ok)
				handler.requestUpdateLock.Unlock()
			}

			if ok {
				break time
			}
			log.Error(err)
		}
	}

	if handler.IsStatRequired {
		handler.RequestSentCount += int64(1)
		if response != nil {
			handler.RequestSuccessCount += int64(1)
		} else {
			handler.RequestFailedCount += int64(1)
		}
	}

	return response
}

func is_Valid_NodeData(member client.Member) bool {
	if (member.Status != "alive") || (member.Tags["Hport"] == "") || (member.Tags["Type"] == "PMDB_SERVER") {
		return false
	}
	return true
}

func (handler *ClientAPIHandler) pick_Server(removeName string) (client.Member, error) {
	handler.tableLock.Lock()
	defer handler.tableLock.Unlock()
	var serverChoosen *client.Member
	switch handler.ServerChooseAlgorithm {
	case 0:
		//Random
		var randomIndex int
		for {
			if len(handler.servers) == 0 {
				log.Error("(CLIENT API MODULE) no alive servers")
				return client.Member{}, errors.New("No alive servers")
			}
			randomIndex = rand.Intn(len(handler.servers))
			if removeName != "" {
				log.Info(removeName)
			}

			//Check if node is alive, check if gossip is available and http server of that node is not reported down!
			if (is_Valid_NodeData(handler.servers[randomIndex])) && (removeName != handler.servers[randomIndex].Name) {
				break
			}
			handler.servers = removeIndex(handler.servers, randomIndex)
		}

		serverChoosen = &handler.servers[randomIndex]
	case 1:
		//Round-Robin
		handler.roundRobinPtr %= len(handler.servers)
		serverChoosen = &handler.servers[handler.roundRobinPtr]
		handler.roundRobinPtr += 1
	case 2:
		//Specific
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

func (handler *ClientAPIHandler) init_serfClient(configPath string) error {
	handler.serfClientObj.Retries = 5
	return handler.serfClientObj.Init_data(configPath)
}

func (handler *ClientAPIHandler) member_Searcher(stop chan int) error {
comparison:
	for {
		select {
		case <-stop:
			log.Info("stopping member updater")
			break comparison
		default:
			//Since we do update it continuesly, we persist the connection
			handler.serfUpdateLock.Lock()
			err := handler.serfClientObj.Update_SerfClient(true)
			handler.serfUpdateLock.Unlock()
			if err != nil {
				log.Error("Unable to connect with agents")
				return err
			}
			handler.tableLock.Lock()
			handler.servers = handler.serfClientObj.Agents
			handler.tableLock.Unlock()
			handler.ready = true
			time.Sleep(1 * time.Second)
		}
	}

	return nil
}

func (handler *ClientAPIHandler) Start_ClientAPI(stop chan int, configPath string) error {
	var err error
	handler.RequestDistribution = make(map[string]*ServerRequestStat)

	//Retry initial serf connect for 5 times
	for i:=0;i<5;i++ {
		err = handler.init_serfClient(configPath)
		if err == nil {
			break
		}
		//Wait for 3 seconds before retrying the connection
		log.Info("Retrying serf agent connection : ",i)
		time.Sleep(3 * time.Second)
	}
	//Return if error persists
	if err!=nil {
		log.Error("Error while initializing the serf client ", err)
		return err
	}

	err = handler.member_Searcher(stop)
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

func (handler *ClientAPIHandler) Get_Config(configPath string) error {
	handler.serfClientObj.Retries = 5
	return handler.serfClientObj.Init_data(configPath)
}

func (handler *ClientAPIHandler) Get_Membership() map[string]client.Member {
        handler.serfUpdateLock.Lock()
        defer handler.serfUpdateLock.Unlock()
        return handler.serfClientObj.Get_MemberList()
}

func (handler *ClientAPIHandler) Get_PMDBServer_Config() ([]byte,error){
	type PeerConfigData struct{
		PeerUUID   string
		IPAddr     string
		Port       string
		ClientPort string
	}
        var PeerUUID, ClientPort, Port, IPAddr string
	PMDBServerConfigMap := make(map[string]PeerConfigData)

	allConfig := handler.serfClientObj.Get_PMDBConfig()
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

func (handler *ClientAPIHandler) Till_ready() {
	for !handler.ready {

	}
}
