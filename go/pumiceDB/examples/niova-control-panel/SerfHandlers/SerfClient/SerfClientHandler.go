package SerfClientHandler

import (
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

	//Un-exported
	agentConnection *client.RPCClient
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
	if Handler.agentConnection == nil {
		flag := 0
		//Retry with different agent addr till getting members list
		for {
			randomAddr = Handler.AgentAddrs[0] //update : Make randomized select
			Handler.agentConnection, err = client.NewRPCClient(randomAddr)
			if err == nil {
				break
			}
			flag += 1
			if flag >= Handler.Retries {
				return nil //change return as cutom error
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
		Handler.agentConnection.Close()
	}

	//Update the data
	Handler.updateTable(clientMembers)
	return nil
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
