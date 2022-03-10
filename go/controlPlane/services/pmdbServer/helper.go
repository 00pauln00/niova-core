package main

import (
	"bufio"
	"common/serfAgent"
	compressionLib "common/specificCompressionLib"
	"errors"
	"flag"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	defaultLogger "log"
	"os"
	"strings"
)

func (handler *pmdbServerHandler) parseArgs() (*NiovaKVServer, error) {

	var err error

	flag.StringVar(&handler.raftUUID, "r", "NULL", "raft uuid")
	flag.StringVar(&handler.peerUUID, "u", "NULL", "peer uuid")

	/* If log path is not provided, it will use Default log path.
	   default log path: /tmp/<peer-uuid>.log
	*/
	defaultLog := "/" + "tmp" + "/" + handler.peerUUID + ".log"
	flag.StringVar(&handler.logDir, "l", defaultLog, "log dir")
	flag.StringVar(&handler.logLevel, "ll", "Info", "Log level")
	flag.StringVar(&handler.gossipClusterFile, "g", "NULL", "Serf agent port")
	flag.Parse()

	nso := &NiovaKVServer{}
	nso.raftUuid = handler.raftUUID
	nso.peerUuid = handler.peerUUID

	if nso == nil {
		err = errors.New("Not able to parse the arguments")
	} else {
		err = nil
	}

	return nso, err
}

func extractPMDBServerConfigfromFile(path string) (*PeerConfigData, string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, "", err
	}
	scanner := bufio.NewScanner(f)
	peerData := PeerConfigData{}
	var compressedConfigString, data string

	for scanner.Scan() {
		text := scanner.Text()
		lastIndex := len(strings.Split(text, " ")) - 1
		key := strings.Split(text, " ")[0]
		value := strings.Split(text, " ")[lastIndex]
		switch key {
		case "CLIENT_PORT":
			peerData.ClientPort = value
			data, err = compressionLib.CompressStringNumber(value, 2)
			compressedConfigString += data
		case "IPADDR":
			peerData.IPAddr = value
			data, err = compressionLib.CompressIPV4(value)
			compressedConfigString += data
		case "PORT":
			peerData.Port = value
			data, err = compressionLib.CompressStringNumber(value, 2)
			compressedConfigString += data
		}
		if err != nil {
			return nil, "", err
		}
	}
	f.Close()

	return &peerData, compressedConfigString, err
}

func (handler *pmdbServerHandler) readPMDBServerConfig() error {
	folder := os.Getenv("NIOVA_LOCAL_CTL_SVC_DIR")
	files, err := ioutil.ReadDir(folder + "/")
	if err != nil {
		return err
	}
	handler.GossipData = make(map[string]string)

	for _, file := range files {
		if strings.Contains(file.Name(), ".peer") {
			//Extract required config from the file
			path := folder + "/" + file.Name()
			peerData, compressedConfigString, err := extractPMDBServerConfigfromFile(path)
			if err != nil {
				return err
			}

			uuid := file.Name()[:len(file.Name())-5]
			cuuid, err := compressionLib.CompressUUID(uuid)
			if err != nil {
				return err
			}

			handler.GossipData[cuuid] = compressedConfigString
			peerData.UUID = uuid
			handler.ConfigData = append(handler.ConfigData, *peerData)
		}
	}

	return nil
}

func (handler *pmdbServerHandler) readGossipClusterFile() error {
	f, err := os.Open(handler.gossipClusterFile)
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		text := scanner.Text()
		splitData := strings.Split(text, " ")
		addr := splitData[1]
		aport := splitData[2]
		rport := splitData[3]
		uuid := splitData[0]
		if uuid == handler.peerUUID {
			handler.aport = aport
			handler.rport = rport
			handler.addr = addr
		} else {
			handler.gossipClusterNodes = append(handler.gossipClusterNodes, addr+":"+aport)
		}
	}

	if handler.addr == "" {
		log.Error("Peer UUID not matching with gossipNodes config file")
		return errors.New("UUID not matching")
	}

	log.Info("Cluster nodes : ", handler.gossipClusterNodes)
	log.Info("Node serf info : ", handler.addr, handler.aport, handler.rport)
	return nil
}

func (handler *pmdbServerHandler) startSerfAgent() error {
	err := handler.readGossipClusterFile()
	if err != nil {
		return err
	}
	serfLog := "00"
	switch serfLog {
	case "ignore":
		defaultLogger.SetOutput(ioutil.Discard)
	default:
		f, err := os.OpenFile("serfLog.log", os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
		if err != nil {
			defaultLogger.SetOutput(os.Stderr)
		} else {
			defaultLogger.SetOutput(f)
		}
	}

	//defaultLogger.SetOutput(ioutil.Discard)
	serfAgentHandler := serfAgent.SerfAgentHandler{
		Name:     handler.peerUUID,
		BindAddr: handler.addr,
	}
	serfAgentHandler.BindPort = handler.aport
	serfAgentHandler.AgentLogger = defaultLogger.Default()
	serfAgentHandler.RpcAddr = handler.addr
	serfAgentHandler.RpcPort = handler.rport
	//Start serf agent
	_, err = serfAgentHandler.SerfAgentStartup(handler.gossipClusterNodes, true)
	if err != nil {
		log.Error("Error while starting serf agent ", err)
	}
	handler.readPMDBServerConfig()
	handler.GossipData["Type"] = "PMDB_SERVER"
	handler.GossipData["Rport"] = handler.rport
	handler.GossipData["RU"] = handler.raftUUID
	log.Info(handler.GossipData)
	serfAgentHandler.SetNodeTags(handler.GossipData)
	return err
}
