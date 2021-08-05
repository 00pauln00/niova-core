package serfclienthandler

import (
	"bufio"
	"errors"
	"math/rand"
	"os"
	"strings"

	"github.com/hashicorp/serf/client"
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
	Agents    []client.Member //Holds all agent names in cluster, initialized with few known agent names
	Retries   int             //No of retries to connect with any agent
	AgentData []client.Member //Holds data of each agent
	//Un-exported
	agentConnection *client.RPCClient
	connectionExist bool
}

/*
Type : Data
Descirption : Holds data about each agent in the cluster
*/
type Data struct {
	Name    string
	Addr    string
	IsAlive bool
	Rport   string
	Tags    map[string]string
}

func (Handler *SerfClientHandler) getConfigData(serfConfigPath string) ([]string, error) {
	//Get addrs and Rports and store it in AgentAddrs and
	if _, err := os.Stat(serfConfigPath); os.IsNotExist(err) {
		return nil, err
	}
	reader, err := os.OpenFile(serfConfigPath, os.O_RDONLY, 0444)
	if err != nil {
		return nil, err
	}
	filescanner := bufio.NewScanner(reader)
	filescanner.Split(bufio.ScanLines)
	var addrs []string
	for filescanner.Scan() {
		input := strings.Split(filescanner.Text(), " ")
		addrs = append(addrs, input[1]+":"+input[3])
	}
	return addrs, nil
}

/*
Type : SerfClientHandler
Method : InitData
Parameters : configPath string
Return value : error
Description : Get configuration data from config file
*/
func (Handler *SerfClientHandler) Initdata(configpath string) error {
	var connectClient *client.RPCClient
	addrs, err := Handler.getConfigData(configpath)
	if err != nil {
		return err
	}
	for _, addr := range addrs {
		connectClient, err = Handler.connectWith(addr)
		if err == nil {
			break
		}
	}

	if connectClient == nil {
		return errors.New("no live agents")
	}

	clusterMembers, err := connectClient.Members()
	Handler.updateTable(clusterMembers)

	return err
}

func (Handler *SerfClientHandler) connectWith(addr string) (*client.RPCClient, error) {
	return client.NewRPCClient(addr)
}
func (Handler *SerfClientHandler) connectRandom() (*client.RPCClient, error) {
	randomIndex := rand.Intn(len(Handler.Agents))
	randomAgent := Handler.Agents[randomIndex]
	randomAddr := randomAgent.Addr.String()
	rPort := randomAgent.Tags["Rport"]
	connector, err := Handler.connectWith(randomAddr + ":" + rPort)
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
func (Handler *SerfClientHandler) GetData(persistConnection bool) error {
	var err error

	//If no connection was persisted
	if !Handler.connectionExist {
		//Retry with different agent addr till getting connected
		for i := 0; i < Handler.Retries; i++ {
			if len(Handler.Agents) <= 0 {
				return errors.New("no live agents")
			}
			Handler.agentConnection, err = Handler.connectRandom()
			if err == nil {
				Handler.connectionExist = true
				break
			}
		}
	}

	//If no connection is made
	if !Handler.connectionExist {
		return errors.New("retry limit exceded") //&RetryLimitExceded{}
	}

	//Get member data from connected agent
	clientMembers, err := Handler.agentConnection.Members()
	if err != nil {
		_ = Handler.agentConnection.Close()
		Handler.connectionExist = false
		return err
	}

	//Close the agent client connection if not to Persist
	if !persistConnection {
		err = Handler.agentConnection.Close()
		Handler.connectionExist = false
	}

	//Update the data
	Handler.updateTable(clientMembers)
	return err
}

/*
Type : SerfClientHandler
Method : updateTable
Parameters : members []client.Member
Return value : None
Description : Updates the local data [node status and tags]
*/
func (Handler *SerfClientHandler) updateTable(members []client.Member) {
	//Delete all addrs
	Handler.Agents = nil
	Handler.AgentData = members
	//Update the Agent data(s)
	for _, mems := range members {
		if mems.Status == "alive" {
			Handler.Agents = append(Handler.Agents, mems)
		}
		//Keep only live members in the list
	}
}
