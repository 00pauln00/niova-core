package main

import PumiceDBServer "niova/go-pumicedb-lib/server"

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
	UUID       string
	ClientPort string
	Port       string
	IPAddr     string
}

type NiovaKVServer struct {
	raftUuid       string
	peerUuid       string
	columnFamilies string
	pso            *PumiceDBServer.PmdbServerObject
}
