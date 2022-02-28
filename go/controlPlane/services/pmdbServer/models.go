package main

type pmdbServerHandler struct {
	raftUUID           string
	peerUUID           string
	logDir             string
	logLevel           string
	gossipClusterFile  string
	gossipClusterNodes []string
	aport              string
	rport              string
	addr               string
	GossipData         map[string]string
	ConfigString       string
	ConfigData         []PeerConfigData
}
type PeerConfigData struct {
	ClientPort string
	Port       string
	IPAddr     string
}
