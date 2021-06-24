package SerfClientHandler

import (
	"math/rand"

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
	AgentAddrs []string         //Holds all agent addr in cluster, initialized with few known agent addr
	Retries    int              //No of retries to connect with any agent
	AgentData  map[string]*Data //Holds data of each agent
	RpcPort    string           //Port for rpc listner
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
	Tags    map[string]string
}

/*
Custom error(s)
*/
type NoLiveAgents struct{}

func (m *NoLiveAgents) Error() string {
	return "No live agents"
}

type RetryLimitExceded struct{}

func (m *RetryLimitExceded) Error() string {
	return "Retry Limit Exceded"
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
	var randomAddr string
	var err error

	//If no connection is persisted
	if !Handler.connectionExist {
		flag := 0
		//Retry with different agent addr till getting members list
		for {
			//Choose random addrs
			if len(Handler.AgentAddrs) <= 0 {
				//Custom error for no live agent to communicate
				return &NoLiveAgents{}
			}
			randomIndex := rand.Intn(len(Handler.AgentAddrs))
			randomAddr = Handler.AgentAddrs[randomIndex]
			Handler.agentConnection, err = client.NewRPCClient(randomAddr + ":" + Handler.RpcPort)
			if err == nil {
				Handler.connectionExist = true
				break
			}
			flag += 1
			//Mark that node as unreachable
			Handler.AgentAddrs = append(Handler.AgentAddrs[:randomIndex], Handler.AgentAddrs[randomIndex+1:]...)
			if flag >= Handler.Retries {
				return &RetryLimitExceded{} //change return as custom error for limit exceeded
			}
		}
	}

	//Get member data from connected agent
	clientMembers, err := Handler.agentConnection.Members()
	if err != nil {
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
	Handler.AgentAddrs = nil

	//Update the Agent data(s)
	for _, mems := range members {
		if Handler.AgentData[mems.Name] == nil {
			Handler.AgentData[mems.Name] = &Data{}
			Handler.AgentData[mems.Name].Name = mems.Name
			Handler.AgentData[mems.Name].Addr = mems.Addr.String()
		}
		Handler.AgentData[mems.Name].IsAlive = true
		Handler.AgentData[mems.Name].Tags = mems.Tags
		//Keep only live members in the list
		Handler.AgentAddrs = append(Handler.AgentAddrs, mems.Addr.String())
	}

}
