package serfClient

import (
	"bufio"
	"errors"
	"github.com/hashicorp/serf/client"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
)

/*
Type : SerfClientHandler
Description : Handler for agent
Methods:
1. GetData
2. updateTable
*/

type SerfClientHandler struct {
	//Exported
	Agents  []client.Member //Holds all agent names in cluster, initialized with few known agent names
	Retries int             //No of retries to connect with any agent
	//Un-exported
	loadedGossipNodes []string
	ipAddrs           net.IP
	portRange         []uint16
	ServicePortRangeS uint16
	ServicePortRangeE uint16
	agentConnection   *client.RPCClient
	connectionExist   bool
}

func (handler *SerfClientHandler) getAddrList() []string {
	var addrs []string
	for i := 0; i <= len(handler.portRange); i++ {
		addrs = append(addrs, handler.ipAddrs.String()+":"+strconv.Itoa(int(handler.portRange[i])))
	}
	return addrs
}

func makeRange(min, max uint16) []uint16 {
	a := make([]uint16, max-min+1)
	for i := range a {
		a[i] = uint16(min + uint16(i))
	}
	return a
}

func (Handler *SerfClientHandler) getConfigData(serfConfigPath string) error {
	//Get addrs and Rports and store it in AgentAddrs and
	/*
		Following is the format of gossipNodes config File
		IPAddrs with space seperated
		Sport Eport
	*/
	if _, err := os.Stat(serfConfigPath); os.IsNotExist(err) {
		return err
	}
	reader, err := os.OpenFile(serfConfigPath, os.O_RDONLY, 0444)
	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(reader)
	//Read IPAddrs
	scanner.Scan()
	IPAddrs := strings.Split(scanner.Text(), " ")
	//TODO Parse IPs Into array
	Handler.ipAddrs = net.ParseIP(IPAddrs[0])

	//Read Ports
	scanner.Scan()
	Ports := strings.Split(scanner.Text(), " ")
	temp, _ := strconv.Atoi(Ports[0])
	Handler.ServicePortRangeS = uint16(temp)
	temp, _ = strconv.Atoi(Ports[1])
	Handler.ServicePortRangeE = uint16(temp)

	Handler.portRange = makeRange(Handler.ServicePortRangeS, Handler.ServicePortRangeE)
	return nil
}

/*
Type : SerfClientHandler
Method : InitData
Parameters : configPath string
Return value : error
Description : Get configuration data from config file
*/
func (Handler *SerfClientHandler) InitData(configpath, raftUUID string) error {
	var connectClient *client.RPCClient
	err := Handler.getConfigData(configpath)
	if err != nil {
		return err
	}

	Handler.loadedGossipNodes = Handler.getAddrList()
	for _, addr := range Handler.loadedGossipNodes {
		connectClient, err = Handler.connectAddr(addr, raftUUID)
		if err == nil {
			break
		}
	}

	if connectClient == nil {
		return errors.New("No live serf agents")
	}

	clusterMembers, err := connectClient.Members()
	Handler.Agents = clusterMembers

	return err
}

func (Handler *SerfClientHandler) connectAddr(addr, raftUUID string) (*client.RPCClient, error) {
	var RPCClientConfig client.Config
	RPCClientConfig.Addr = addr
	RPCClientConfig.AuthKey = raftUUID
	return client.ClientFromConfig(&RPCClientConfig)
}

func (Handler *SerfClientHandler) connectRandomNode(raftUUID string) (*client.RPCClient, error) {
	randomIndex := rand.Intn(len(Handler.Agents))
	randomAgent := Handler.Agents[randomIndex]
	randomAddr := randomAgent.Addr.String()
	rPort := randomAgent.Tags["Rport"]
	connector, err := Handler.connectAddr(randomAddr+":"+rPort, raftUUID)
	if err != nil {
		//Delete the node from connection list
		Handler.Agents = append(Handler.Agents[:randomIndex], Handler.Agents[randomIndex+1:]...)
	}
	return connector, err
}

/*
Type : SerfClientHandler
Method : GetData
Parameters : persistConnection bool
Return value : error
Description : Gets data from a random agent, persist the agent connection if persistConnection is true.
persistConnection can be used if frequect updates are required.
*/
func (Handler *SerfClientHandler) UpdateSerfClient(persistConnection bool, raftUUID string) error {
	var err error

	//If no connection was persisted
	if !Handler.connectionExist {
		//Retry with different agent addr till getting connected
		for i := 0; i < Handler.Retries; i++ {
			if len(Handler.Agents) <= 0 {
				return errors.New("No live serf agents")
			}
			Handler.agentConnection, err = Handler.connectRandomNode(raftUUID)
			if err == nil {
				Handler.connectionExist = true
				break
			}
		}
	}

	//If no connection is made
	if !Handler.connectionExist {
		return errors.New("serf agent connection retry limit exceded")
	}

	//Get member data from connected agent
	clusterMembers, err := Handler.agentConnection.Members()
	if err != nil {
		_ = Handler.agentConnection.Close()
		Handler.connectionExist = false
		return err
	}
	Handler.Agents = clusterMembers

	//Close the agent client connection if not to Persist
	if !persistConnection {
		err = Handler.agentConnection.Close()
		Handler.connectionExist = false
	}

	//Update the data
	return err
}

func (Handler *SerfClientHandler) GetPMDBConfig() string {
	for _, mem := range Handler.Agents {
		if mem.Tags["Type"] == "PMDB_SERVER" {
			return mem.Tags["PC"]
		}
	}
	return ""
}

func (Handler *SerfClientHandler) GetTags(filterKey string, filterValue string) map[string]map[string]string {
	returnMap := make(map[string]map[string]string)
	for _, mem := range Handler.Agents {
		if mem.Tags[filterKey] == filterValue {
			returnMap[mem.Name] = mem.Tags
		}
	}
	return returnMap
}

/*
Type : SerfClientHandler
*/
func (Handler *SerfClientHandler) GetMemberList() map[string]client.Member {
	memberMap := make(map[string]client.Member)
	for _, mems := range Handler.Agents {
		memberMap[mems.Name] = mems
	}
	return memberMap
}
