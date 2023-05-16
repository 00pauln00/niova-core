package main

import (
	leaseServerLib "LeaseLib/leaseServer"
	"errors"
	"flag"
	PumiceDBCommon "niova/go-pumicedb-lib/common"
	PumiceDBServer "niova/go-pumicedb-lib/server"
	"os"

	log "github.com/sirupsen/logrus"
)

/*
#cgo LDFLAGS: -lniova -lniova_raft -lniova_pumice -lrocksdb
#include <stdlib.h>
#include <string.h>
#include <raft/pumice_db.h>
#include <raft/raft_net.h>
#include <raft/pumice_db_client.h>
*/
import "C"

var seqno = 0
var ttlDefault = 60

// Use the default column family
var colmfamily []string = []string{"PMDBTS_CF"}

type leaseServer struct {
	raftUUID       string
	peerUUID       string
	logDir         string
	logLevel       string
	leaseObj       leaseServerLib.LeaseServerObject
	pso            *PumiceDBServer.PmdbServerObject
	coverageOutDir string
}

func main() {
	lso, pErr := parseArgs()
	if pErr != nil {
		log.Println("Invalid args - ", pErr)
		return
	}

	switch lso.logLevel {
	case "Info":
		log.SetLevel(log.InfoLevel)
	case "Trace":
		log.SetLevel(log.TraceLevel)
	}

	//Create log file
	err := PumiceDBCommon.InitLogger(lso.logDir)
	if err != nil {
		log.Error("Error while initating logger ", err)
		os.Exit(1)
	}

	if err != nil {
		log.Fatal("Error while initializing serf agent ", err)
	}

	log.Info("Raft and Peer UUID: ", lso.raftUUID, " ", lso.peerUUID)
	/*
	   Initialize the internal pmdb-server-object pointer.
	   Assign the Directionary object to PmdbAPI so the apply and
	   read callback functions can be called through pmdb common library
	   functions.
	*/
	lso.pso = &PumiceDBServer.PmdbServerObject{
		RaftUuid:       lso.raftUUID,
		PeerUuid:       lso.peerUUID,
		PmdbAPI:        lso,
		SyncWrites:     false,
		CoalescedWrite: true,
		LeaseEnabled:   true,
	}

	//TODO: Fill all fields of LeaseServerObject
	lso.leaseObj = leaseServerLib.LeaseServerObject{}
	lso.leaseObj.InitLeaseObject(lso.pso)

	// For lease application use lease column family
	lso.pso.ColumnFamilies = []string{lso.leaseObj.LeaseColmFam}
	go PumiceDBCommon.EmitCoverDataNKill(lso.coverageOutDir)
	// Start the pmdb server
	err = lso.pso.Run()
	if err != nil {
		log.Error(err)
	}
}

func usage() {
	flag.PrintDefaults()
	os.Exit(0)
}

func parseArgs() (*leaseServer, error) {

	var err error
	lso := &leaseServer{}

	flag.StringVar(&lso.raftUUID, "r", "NULL", "raft uuid")
	flag.StringVar(&lso.peerUUID, "u", "NULL", "peer uuid")

	/* If log path is not provided, it will use Default log path.
	   default log path: /tmp/<peer-uuid>.log
	*/
	defaultLog := "/" + "tmp" + "/" + lso.peerUUID + ".log"
	flag.StringVar(&lso.logDir, "l", defaultLog, "log dir")
	flag.StringVar(&lso.logLevel, "ll", "Info", "Log level")
	flag.StringVar(&lso.coverageOutDir, "cov", "", "Path to write code coverage data")
	flag.Parse()

	if lso == nil {
		err = errors.New("Not able to parse the arguments")
	} else {
		err = nil
	}

	return lso, err
}

func (lso *leaseServer) WritePrep(wrPrepArgs *PumiceDBServer.PmdbCbArgs) int64 {
	return 0
}

func (lso *leaseServer) Apply(applyArgs *PumiceDBServer.PmdbCbArgs) int64 {
	return 0
}

func (lso *leaseServer) Read(readArgs *PumiceDBServer.PmdbCbArgs) int64 {
	return 0
}

func (lso *leaseServer) Init(initPeerArgs *PumiceDBServer.PmdbCbArgs) {
	return
}

func (lso *leaseServer) PrepPeer(prepPeer *PumiceDBServer.PmdbCbArgs) {
	return
}
