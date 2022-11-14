package serfAgent

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	//	"errors"
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
	Name              string //Name of the agent
	BindAddr          net.IP //Addr for inter agent communcations
	RpcAddr           net.IP //Addr for agent-client communication
	AgentLogger       *log.Logger
	ServicePortRangeS uint16
	ServicePortRangeE uint16
	RpcPort           uint16
	//non-exported
	joinAddrs   []string
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
	serfconfig.MemberlistConfig.BindPort = int(Handler.ServicePortRangeS)                 //Agent bind port
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
	var err error
	for i := Handler.ServicePortRangeS; i < Handler.ServicePortRangeE; i++ {
		Handler.setup()
		err = Handler.agentObj.Start()
		if err != nil {
			Handler.ServicePortRangeS += 1
			continue
		} else {
			fmt.Println("Succesfully binded to port - ", Handler.ServicePortRangeS)
			break
		}
	}

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

	var rpcListener net.Listener
	for i := Handler.ServicePortRangeS; i < Handler.ServicePortRangeE; i++ {
		Handler.RpcPort = Handler.ServicePortRangeS
		rpcListener, err = net.Listen("tcp", Handler.RpcAddr.String()+":"+strconv.Itoa(int(Handler.RpcPort)))
		if err != nil {
			Handler.ServicePortRangeS += 1
			continue
		} else {
			fmt.Println("Succesfully binded RPC Port to - ", Handler.RpcPort)
			break
		}
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
	fmt.Println("Serf agent found ", no_of_nodes, " in the cluster")
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
	if len(Handler.joinAddrs) != 0 {
		memcount, _ = Handler.join(Handler.joinAddrs)
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
Method name : GetMembersState
Parameters : None
Return value : map[stirng]bool
Description : Returns state of nodes in cluster
*/
func (Handler *SerfAgentHandler) GetMembersState() map[string]bool {
	memberState := make(map[string]bool)
	members := Handler.agentObj.Serf().Members()
	for _, mems := range members {
		if mems.Status == 1 {
			memberState[mems.Name] = true
		} else {
			memberState[mems.Name] = false
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

func (Handler *SerfAgentHandler) getServerAddress(staticSerfConfigPath string) error {
	//Get addrs and Rports and store it in AgentAddrs and

	if _, err := os.Stat(staticSerfConfigPath); os.IsNotExist(err) {
		return err
	}
	reader, err := os.OpenFile(staticSerfConfigPath, os.O_RDONLY, 0444)
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(reader)
	scanner.Scan()
	IPAddrs := scanner.Text()
	Handler.joinAddrs = strings.Split(IPAddrs, " ")

	//Read Ports
	scanner.Scan()
	Ports := strings.Split(scanner.Text(), " ")
	temp, _ := strconv.Atoi(Ports[0])
	Handler.ServicePortRangeS = uint16(temp)
	temp, _ = strconv.Atoi(Ports[1])
	Handler.ServicePortRangeE = uint16(temp)
	return nil
}
