package serfAgent

import (
	"bufio"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

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
	Name        string //Name of the agent
	BindAddr    net.IP //Addr for inter agent communcations
	BindPort    uint16 //Port for inter agent communcations
	RpcAddr     net.IP //Addr for agent-client communication
	RpcPort     uint16 //Port for agent-client communicaton
	AgentLogger *log.Logger

	//non-exported
	agentObj    *agent.Agent
	agentIPCObj *agent.AgentIPC
	agentConf   *agent.Config
}

/*
Type : SerfAgentHandler
Method name : setup
Parameters : None
Return values : error
Description : Creates agent with configurations mentioned in structure and returns error if any, it dosent start the agent
*/
func (Handler *SerfAgentHandler) setup() error {

	//Init an agent
	serfconfig := serf.DefaultConfig()                                                    //config for serf
	serfconfig.NodeName = Handler.Name                                                    //Agent name
	serfconfig.MemberlistConfig.BindAddr = Handler.BindAddr.String()                      //Agent bind addr
	serfconfig.MemberlistConfig.BindPort = int(Handler.BindPort)                          //Agent bind port
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
func (Handler *SerfAgentHandler) start(requireRPC bool) error {
	err := Handler.agentObj.Start()

	if !requireRPC {
		return err
	}

	//Create func handler for rpc, client handlers are binded to rpc.
	FuncHandler := &agent.ScriptEventHandler{
		SelfFunc: func() serf.Member { return Handler.agentObj.Serf().LocalMember() },
		Scripts:  Handler.agentConf.EventScripts(),
		Logger:   Handler.AgentLogger,
	}
	Handler.agentObj.RegisterEventHandler(FuncHandler)

	//Return if error in starting the agent
	if err != nil {
		return err
	}

	//Start a RPC listener
	agentLog := agent.NewLogWriter(10) //Need change for logging
	rpcListener, err := net.Listen("tcp", Handler.RpcAddr.String()+":"+strconv.Itoa(int(Handler.RpcPort)))
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
	//fmt.Println(Handler.agentObj)
	no_of_nodes, err := Handler.agentObj.Join(addrs, false) //Change with deployment add :Handler.Bindport
	return no_of_nodes, err
}

func GetPeerAddress(staticSerfConfigPath string) ([]string, error) {
	//Get addrs and Rports and store it in AgentAddrs and
	if _, err := os.Stat(staticSerfConfigPath); os.IsNotExist(err) {
		return nil, err
	}
	reader, err := os.OpenFile(staticSerfConfigPath, os.O_RDONLY, 0444)
	if err != nil {
		return nil, err
	}
	filescanner := bufio.NewScanner(reader)
	filescanner.Split(bufio.ScanLines)
	var addrs []string
	for filescanner.Scan() {
		input := strings.Split(filescanner.Text(), " ")
		addrs = append(addrs, input[1]+":"+input[2])
	}
	return addrs, nil
}

/*
Type : SerfAgentHandler
Method name : Startup
Parameters : staticSerfConfigPath
Return value : int, error
Description : Does setup, start and joins in cluster
*/
func (Handler *SerfAgentHandler) SerfAgentStartup(joinAddrs []string, RPCRequired bool) (int, error) {
	var err error
	var memcount int
	//Setup
	err = Handler.setup()
	if err != nil {
		return 0, err
	}
	//Start agent and RPC server
	err = Handler.start(RPCRequired)
	if err != nil {
		return 0, err
	}

	//Join the cluster
	if len(joinAddrs) != 0 {
		memcount, _ = Handler.join(joinAddrs)
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
func (Handler *SerfAgentHandler) GetMembership() []string {
	var membersAddr []string
	members := Handler.agentObj.Serf().Members()
	for _, mems := range members {
		membersAddr = append(membersAddr, mems.Addr.String())
	}
	return membersAddr
}

/*
Type : SerfAgentHandler
Method name : GetMembersStatus
Parameters : None
Return value : map[stirng]bool
Description : Returns status of nodes in cluster
*/
func (Handler *SerfAgentHandler) GetMembersStatus() map[string]bool {
	memberStatus := make(map[string]bool)
	members := Handler.agentObj.Serf().Members()
	for _, mems := range members {
		if mems.Status == 1 {
			memberStatus[mems.Name] = true
		} else {
			memberStatus[mems.Name] = false
		}
	}
	return memberStatus
}


/*
Type : SerfAgentHandler
Method name : GetMemberState
Parameters : None
Return value : map[string]int
Description : Returns state of the nodes in cluster in int type
*/
func (Handler *SerfAgentHandler) GetMemberState() map[string]int {
	memberState := make(map[string]int)
	members := Handler.agentObj.Serf().Members()
	for _, mems := range members {
		if mems.Status == 1 {
			memberState[mems.Name], _ = strconv.Atoi(mems.Tags["State"])
		} else {
			memberState[mems.Name] = 0
		}
	}
	return memberState
}

/*
Type : SerfAgentHandler
Method name : SetTags
Parameters : tags (map[string]string)
Return value : error
Description : Update tags, its incremental type update
*/
func (Handler *SerfAgentHandler) SetNodeTags(tags map[string]string) error {
	err := Handler.agentObj.SetTags(tags)
	return err
}

func (Handler *SerfAgentHandler) GetTags(filterKey string, filterValue string) map[string]map[string]string {
	members := Handler.agentObj.Serf().Members()
	returnMap := make(map[string]map[string]string)
	for _, mem := range members {
		if mem.Tags[filterKey] == filterValue {
			returnMap[mem.Name] = mem.Tags
		}
	}
	return returnMap
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

/*
Type : SerfAgentHandler
Method name : getServerAddress
Parameters : staticSerfConfigPath
Return Value : list of addrs
Description : Get the list of teh addresses to join the cluster.
*/

func (Handler *SerfAgentHandler) getServerAddress(staticSerfConfigPath string) ([]string, error) {
	//Get addrs and Rports and store it in AgentAddrs and

	if _, err := os.Stat(staticSerfConfigPath); os.IsNotExist(err) {
		return nil, err
	}
	reader, err := os.OpenFile(staticSerfConfigPath, os.O_RDONLY, 0444)
	if err != nil {
		return nil, err
	}
	filescanner := bufio.NewScanner(reader)
	filescanner.Split(bufio.ScanLines)
	var addrs []string
	for filescanner.Scan() {
		input := strings.Split(filescanner.Text(), " ")
		addrs = append(addrs, input[0]+":"+input[1])
	}
	return addrs, nil
}
