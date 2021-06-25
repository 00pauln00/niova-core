package serfagenthandler

import (
	"log"
	"net"

	"github.com/hashicorp/serf/cmd/serf/command/agent"
	"github.com/hashicorp/serf/serf"
)

/*
Struct name : SerfAgentHandler
Description : Handler for agent
Methods:
1. setup
2. start
3. join
4. Startup
5. GetMembers
6. SetTags
7. Close
*/
type SerfAgentHandler struct {
	//Exported
	Name     string //Name of the agent
	BindAddr string //Addr for inter agent communcations
	BindPort int    //Port for inter agent communcations
	RpcAddr  string //Addr for agent-client communication
	RpcPort  string //Port for agent-client communication
	//Tags        map[string]string //Meta data for the agent, which is replication on all agents in the cluster
	AgentLogger *log.Logger

	//non-exported
	agentObj    *agent.Agent
	agentIPCObj *agent.AgentIPC
	agentConf   *agent.Config
}

/*
Type : SerfAgentHandler
Method name : Setup
Parameters : None
Return values : error
Description : Creates agent with configurations mentioned in structure and returns error if any, it dosent start the agent
*/
func (Handler *SerfAgentHandler) setup() error {

	//Init an agent
	serfconfig := serf.DefaultConfig()                                                    //config for serf
	serfconfig.NodeName = Handler.Name                                                    //Agent name
	serfconfig.MemberlistConfig.BindAddr = Handler.BindAddr                               //Agent bind addr
	serfconfig.MemberlistConfig.BindPort = Handler.BindPort                               //Agent bind port
	agentconfig := agent.DefaultConfig()                                                  //Agent config to provide for agent creation
	serfagent, err := agent.Create(agentconfig, serfconfig, Handler.AgentLogger.Writer()) //Agent creation; last parameter is log, need to check that

	//Create SerfAgentHandler obj and init the values
	Handler.agentObj = serfagent
	Handler.agentConf = agentconfig

	return err
}

/*
Type : SerfAgentHandler
Method name : Start
Parameters : None
Return Value : error
Description : Starts the created agent in setup, and listenes on rpc channel
*/
func (Handler *SerfAgentHandler) start() error {

	//Create func handler for rpc, client handlers are binded to rpc.
	FuncHandler := &agent.ScriptEventHandler{
		SelfFunc: func() serf.Member { return Handler.agentObj.Serf().LocalMember() },
		Scripts:  Handler.agentConf.EventScripts(),
		Logger:   Handler.AgentLogger,
	}
	Handler.agentObj.RegisterEventHandler(FuncHandler)
	err := Handler.agentObj.Start()

	//Return if error in starting the agent
	if err != nil {
		return err
	}

	//Start a RPC listener
	agentLog := agent.NewLogWriter(10) //Need change for logging
	rpcListener, err := net.Listen("tcp", Handler.RpcAddr+":"+Handler.RpcPort)
	if err != nil {
		return err
	}
	Handler.agentIPCObj = agent.NewAgentIPC(Handler.agentObj, "", rpcListener, Handler.AgentLogger.Writer(), agentLog) //Need change for logging

	return nil
}

/*
Type : SerfAgentHandler
Method name : join
Parameters : Addrs []string
Return value : int, error
Description : Joins the cluster
*/
func (Handler *SerfAgentHandler) join(addrs []string) (int, error) {
	no_of_nodes, err := Handler.agentObj.Join(addrs, false)
	return no_of_nodes, err
}

/*
Type : SerfAgentHandler
Method name : Startup
Parameters : joinaddrs []string
Return value : int, error
Description : Does setup, start and joins in cluster
*/
func (Handler *SerfAgentHandler) Startup(joinaddrs []string) (int, error) {
	var err error
	var memcount int
	//Setup
	err = Handler.setup()
	if err != nil {
		return 0, err
	}
	//Start agent and RPC server
	err = Handler.start()
	if err != nil {
		return 0, err
	}
	//Join the cluster
	if len(joinaddrs) != 0 {
		memcount, err = Handler.join(joinaddrs)
	}
	return memcount, err
}

/*
Type : SerfAgentHandler
Method name : GetMembers
Parameters : None
Return value : Members []string
Description : Returns addr of nodes in the cluster
*/
func (Handler *SerfAgentHandler) GetMembers() []string {
	var membersAddr []string
	members := Handler.agentObj.Serf().Members()
	for _, mems := range members {
		membersAddr = append(membersAddr, mems.Addr.String())
	}
	return membersAddr
}

/*
Type : SerfAgentHandler
Method name : SetTags
Parameters : tags (map[string]string)
Return value : error
Description : Update tags, its incremental type update
*/
func (Handler *SerfAgentHandler) SetTags(tags map[string]string) error {
	err := Handler.agentObj.SetTags(tags)
	return err
}

/*
Type : SerfAgentHandler
Method name : Close
Parameters : None
Return Value : None
Description : Stops and closes the agent and stops listenting on bind addr and rpc addr
*/
func (Handler *SerfAgentHandler) Close() error {
	Handler.agentIPCObj.Shutdown()
	err := Handler.agentObj.Shutdown()
	return err
}
