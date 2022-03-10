package main

import (
	"os"

	log "github.com/sirupsen/logrus"
)

func getAnyEntryFromStringMap(mapSample map[string]map[string]string) map[string]string {
	for _, v := range mapSample {
		return v
	}
	return nil
}

func (handler *proxyHandler) dumpConfigToFile(outfilepath string) error {
	//Generate .raft
	raft_file, err := os.Create(outfilepath + handler.raftUUID + ".raft")
	if err != nil {
		return err
	}

	_, errFile := raft_file.WriteString("RAFT " + handler.raftUUID + "\n")
	if errFile != nil {
		return err
	}

	for _, peer := range handler.PMDBServerConfigArray {
		raft_file.WriteString("PEER " + peer.PeerUUID + "\n")
	}

	raft_file.Sync()
	raft_file.Close()

	//Generate .peer
	for _, peer := range handler.PMDBServerConfigArray {
		peer_file, err := os.Create(outfilepath + peer.PeerUUID + ".peer")
		if err != nil {
			log.Error(err)
		}

		_, errFile := peer_file.WriteString(
			"RAFT         " + handler.raftUUID +
				"\nIPADDR       " + peer.IPAddr +
				"\nPORT         " + peer.Port +
				"\nCLIENT_PORT  " + peer.ClientPort +
				"\nSTORE        ./*.raftdb\n")

		if errFile != nil {
			return errFile
		}
		peer_file.Sync()
		peer_file.Close()
	}
	return nil
}
