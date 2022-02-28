package main

import (
	"bufio"
	"common/serfAgent"
	compressionLib "common/specificCompressionLib"
	"errors"
	"flag"
	"io/ioutil"
	defaultLogger "log"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
)

func usage() {
	flag.PrintDefaults()
	os.Exit(0)
}

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

func (handler *pmdbServerHandler) readPMDBServerConfig() {
	folder := os.Getenv("NIOVA_LOCAL_CTL_SVC_DIR")
	files, err := ioutil.ReadDir(folder + "/")
	if err != nil {
		return
	}

	for _, file := range files {
		if strings.Contains(file.Name(), ".peer") {
			f, _ := os.Open(folder + "/" + file.Name())
			scanner := bufio.NewScanner(f)
			peerData := PeerConfigData{}
			uuid := file.Name()[:len(file.Name())-5] + "/"
			cuuid, _ := compressionLib.CompressUUID(uuid)
			handler.GossipData[cuuid] = ""
			for scanner.Scan() {
				text := scanner.Text()
				lastIndex := len(strings.Split(text, " ")) - 1
				key := strings.Split(text, " ")[0]
				value := strings.Split(text, " ")[lastIndex]
				switch key {
				case "CLIENT_PORT":
					peerData.ClientPort = value
					compressedData, _ := compressionLib.CompressStringNumber(value, 2)
					handler.GossipData[cuuid] += compressedData
				case "IPADDR":
					peerData.IPAddr = value
					compressedData, _ := compressionLib.CompressIPV4(value)
					handler.GossipData[cuuid] += compressedData
				case "PORT":
					peerData.Port = value
					compressedCport, _ := compressionLib.CompressStringNumber(value, 2)
					handler.GossipData[cuuid] += compressedCport
				}
			}
			f.Close()
			handler.ConfigData = append(handler.ConfigData, peerData)
		}
	}
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
	handler.GossipData = make(map[string]string)
	handler.GossipData["Type"] = "PMDB_SERVER"
	handler.GossipData["Rport"] = handler.rport
	handler.GossipData["RU"] = handler.raftUUID
	serfAgentHandler.SetNodeTags(handler.GossipData)
	return err
}
