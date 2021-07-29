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
	Agents    []string         //Holds all agent names in cluster, initialized with few known agent names
	Retries   int              //No of retries to connect with any agent
	AgentData map[string]*Data //Holds data of each agent
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

func (Handler *SerfClientHandler) getConfigData(serfConfigPath string) error {
	//Get addrs and Rports and store it in AgentAddrs and
	if _, err := os.Stat(serfConfigPath); os.IsNotExist(err) {
		return err
	}
	reader, err := os.OpenFile(serfConfigPath, os.O_RDONLY, 0444)
	if err != nil {
		return err
	}
	filescanner := bufio.NewScanner(reader)
	filescanner.Split(bufio.ScanLines)
	for filescanner.Scan() {
		input := strings.Split(filescanner.Text(), " ")
		Handler.AgentData[input[0]] = &Data{}
		Handler.AgentData[input[0]].Name = input[0]
		Handler.AgentData[input[0]].Addr = input[1]
		Handler.AgentData[input[0]].Rport = input[3]
		Handler.AgentData[input[0]].IsAlive = false
		Handler.Agents = append(Handler.Agents, input[0])
	}
	return nil
}

/*
Type : SerfClientHandler
Method : InitData
Parameters : configPath string
Return value : error
Description : Get configuration data from config file
*/
func (Handler *SerfClientHandler) Initdata(configpath string) error {
	return Handler.getConfigData(configpath)
}

func (Handler *SerfClientHandler) connect() (*client.RPCClient, error) {
	randomIndex := rand.Intn(len(Handler.Agents))
	randomAgent := Handler.Agents[randomIndex]
	randomAddr := Handler.AgentData[randomAgent].Addr
	connector, err := client.NewRPCClient(randomAddr + ":" + Handler.AgentData[randomAgent].Rport)
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

	//If no connection is persisted
	if !Handler.connectionExist {
		//Retry with different agent addr till getting connected
		for i := 0; i < Handler.Retries; i++ {
			if len(Handler.Agents) <= 0 {
				return errors.New("No live agents")
			}
			Handler.agentConnection, err = Handler.connect()
			if err == nil {
				Handler.connectionExist = true
				break
			}
		}
	}

	//If no connection is made
	if !Handler.connectionExist {
		return errors.New("Retry Limit Exceded") //&RetryLimitExceded{}
	}

	//Get member data from connected agent
	clientMembers, err := Handler.agentConnection.Members()
	if err != nil {
		_ = Handler.agentConnection.Close()
		Handler.connectionExist = false
		return err
	}

	//Close the agent client connection if not Persist connection
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
	//Mark all node as failed
	for _, mems := range Handler.AgentData {
		mems.IsAlive = false
	}

	//Delete all addrs
	Handler.Agents = nil

	//Update the Agent data(s)
	for _, mems := range members {
		if mems.Status == "alive" {
			nodeName := mems.Name
			if Handler.AgentData[nodeName] == nil {
				Handler.AgentData[nodeName] = &Data{}
				Handler.AgentData[nodeName].Name = mems.Name
				Handler.AgentData[nodeName].Addr = mems.Addr.String()
			}
			Handler.AgentData[nodeName].IsAlive = true
			Handler.AgentData[nodeName].Tags = mems.Tags
			Handler.AgentData[nodeName].Rport = mems.Tags["Rport"]
			//Keep only live members in the list
			Handler.Agents = append(Handler.Agents, nodeName)
		}
	}
}
