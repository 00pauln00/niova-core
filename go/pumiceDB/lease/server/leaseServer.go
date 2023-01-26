package leaseServer

import (
	PumiceDBServer "niova/go-pumicedb-lib/server"
	uuid "github.com/google/uuid"
        log "github.com/sirupsen/logrus"
	leaseLib "common/leaseLib"
)



type leaseServer struct {
        leaseMap map[uuid.UUID]*common.LeaseStruct
        pso      *PumiceDBServer.PmdbServerObject
}



//Helper functions
func checkMajorCorrectness(currentTerm int, leaseTerm int) {
        if(currentTerm != leaseTerm) {
                log.Fatal("Major(Term) not matching")
        }
}

func isPermitted(entry *leaseLib.LeaseStruct, clientUUID uuid.UUID, currentTime leaseLib.LeaderTS, operation int) bool {
        if entry.LeaseState == requestResponseLib.INPROGRESS {
                return false
        }
        leaseExpiryTS := entry.TimeStamp.LeaderTime + int64(entry.TTL)

        //Check majot correctness
	checkMajorCorrectness(currentTime.TimeStamp.LeaderTerm, entry.TimeStamp.LeaderTerm)
        //Check if existing lease is valid; by comparing only the minor
        stillValid := leaseExpiryTS >= currentTime.LeaderTime
	//If valid, Check if client uuid is same and operation is refresh
        if stillValid {
                if (operation == leaseLib.REFRESH) && (clientUUID == entry.Client) {
                        return true
                } else {
                        return false
                }
        }
        return true
}

func (lso *leaseServer) Prepare(opcode int, resourceUUID uuid.UUID, clientUUID uuid.UUID, reply *interface{}) int {
	var currentTime leaseLib.LeaderTS
        lso.GetLeaderTimeStamp(&currentTime)

        //Check if its a refresh request
        if opcode == leaseLib.REFRESH {
		vdev_lease_info, isPresent := lso.leaseMap[resourceUUID]
                if isPresent {
                        if isPermitted(vdev_lease_info, clientUUID, currentTime, opcode) {
                                //Refresh the lease
                                lso.leaseMap[resourceUUID].TimeStamp = currentTime
                                lso.leaseMap[resourceUUID].TTL = ttlDefault
                                //Copy the encoded result in replyBuffer
                               	*reply = *lso.leaseMap[resourceUUID]
                                return 1
                        }
                }
                return 0
        }

        //Check if get lease
        if opcode == leaseLib.GET {
                vdev_lease_info, isPresent := lso.leaseMap[resourceUUID]	
		log.Info("Get lease operation")
                if isPresent {
                        if !isPermitted(vdev_lease_info, clientUUID, currentTime, opcode) {
                                //Dont provide lease
                                return 0
                        }
                }
                log.Info("Resource not present in map")
                //Insert or update into MAP
                lso.leaseMap[resourceUUID] = &leaseLib.LeaseStruct{
                        Resource:   resourceUUID,
                        Client:     clientUUID,
                        LeaseState: leaseLib.INPROGRESS,
                }
        }

        return 1	
}



func (lso *leaseServer) ReadLease(resourceUUID uuid.UUID, reply *interface{}) int {
	leaseObj, isPresent := lso.leaseMap[resourceUUID]
	if !isPresent {
		return -1
	}

	if leaseObj.LeaseState == leaseLib.INPROGRESS {
                        return -1
        }

        oldTS := leaseObj.TimeStamp
        lso.GetLeaderTimeStamp(&leaseObj.TimeStamp)
        checkMajorCorrectness(leaseObj.TimeStamp.LeaderTerm, oldTS.LeaderTerm)
	
	//TODO: Possible wrap around
        ttl := ttlDefault - int(leaseObj.TimeStamp.LeaderTime - oldTS.LeaderTime)
        if ttl < 0 {
        	leaseObj.TTL = 0
                leaseObj.LeaseState = leaseLib.EXPIRED
        } else {
               	leaseObj.TTL = ttl
        }
	*reply = *leaseObj
	return 0
}

func (lso *leaseServer) ApplyLease(resourceUUID uuid.UUID, clientUUID uuid.UUID, reply *interface{}) int {
	leaseObj, isPresent := lso.leaseMap[resourceUUID]
        if !isPresent {
                leaseObj = &leaseLib.LeaseStruct{
                        Resource: resourceUUID,
                        Client:   clientUUID,
                }
                lso.leaseMap[resourceUUID] = leaseObj
        }
        leaseObj.LeaseState = leaseLib.GRANTED
        isLeaderFlag := lso.GetLeaderTimeStamp(&leaseObj.TimeStamp)
        leaseObj.TTL = ttlDefault	
}
