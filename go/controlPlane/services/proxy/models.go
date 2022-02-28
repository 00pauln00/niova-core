package main

import (
	"common/httpServer"
	"common/serfAgent"
	pmdbClient "niova/go-pumicedb-lib/client"
)

type proxyHandler struct {
	//Other
	configPath string
	logLevel   string

	//Niovakvserver
	addr string

	//Pmdb nivoa client
	raftUUID                string
	clientUUID              string
	logPath                 string
	PMDBServerConfigArray   []PeerConfigData
	PMDBServerConfigByteMap map[string][]byte
	pmdbClientObj           *pmdbClient.PmdbClientObj

	//Serf agent
	serfAgentName     string
	serfAgentPort     string
	serfAgentRPCPort  string
	serfPeersFilePath string
	serfLogger        string
	serfAgentObj      serfAgent.SerfAgentHandler

	//Http
	httpPort      string
	limit         string
	requireStat   string
	httpServerObj httpServer.HTTPServerHandler
}

type PeerConfigData struct {
	PeerUUID   string
	ClientPort string
	Port       string
	IPAddr     string
}
