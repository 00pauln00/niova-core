package leaseServer

import (
	"bytes"
	leaseLib "common/leaseLib"
	list "container/list"
	"encoding/gob"
	PumiceDBServer "niova/go-pumicedb-lib/server"
	"sync"
	"time"
	"unsafe"
	uuid "github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
)

var ttlDefault = 60
var gcTimeout = 35
var LEASE_COLUMN_FAMILY = "NIOVA_LEASE_CF"


const (
	//Go return codes
	ERROR	    int = -1
	SEND_RESPONSE	= 0
	CONTINUE_WR	= 1
)

type LeaseServerObject struct {
	LeaseColmFam string
	LeaseMap     map[uuid.UUID]*leaseLib.LeaseInfo
	Pso          *PumiceDBServer.PmdbServerObject
	listObj      *list.List
	listLock     sync.RWMutex
}

type LeaseServerReqHandler struct {
	LeaseServerObj *LeaseServerObject
	LeaseReq       leaseLib.LeaseReq
	LeaseRes       *leaseLib.LeaseRes
	UserID         unsafe.Pointer
	PmdbHandler    unsafe.Pointer
}

//Helper functions
func checkMajorCorrectness(currentTerm int64, leaseTerm int64) {
	if currentTerm != leaseTerm {
		log.Fatal("Major(Term) not matching")
	}
}

func copyToResponse(l *leaseLib.LeaseMeta, r *leaseLib.LeaseRes) {
	r.Resource = l.Resource
	r.Client = l.Client
	r.LeaseState = l.LeaseState
	r.TTL = l.TTL
	r.TimeStamp = l.TimeStamp
}

func isPermitted(entry *leaseLib.LeaseMeta, clientUUID uuid.UUID, currentTime leaseLib.LeaderTS, operation int) bool {
	if (entry.LeaseState == leaseLib.INPROGRESS) || (entry.LeaseState == leaseLib.STALE_INPROGRESS) {
		return false
	}

	//Check major correctness
	checkMajorCorrectness(currentTime.LeaderTerm, entry.TimeStamp.LeaderTerm)

	//Check if existing lease is valid; by comparing only the minor
	le := entry.TimeStamp.LeaderTime + int64(entry.TTL)
	sv := le > currentTime.LeaderTime
	if sv {
		// if lease is valid and operation from same client, allow it
		if (clientUUID == entry.Client) {
			return true
		}
		return false
	} else {
		// If lease expired but refresh allowed from same client only
		if (operation == leaseLib.REFRESH) && (clientUUID == entry.Client) {
			return true
		} else if (operation == leaseLib.GET) {
			//GET allowed on expired lease from any client
			return true
		}
	}

	return false
}

func Decode(payload []byte) (leaseLib.LeaseReq, error) {
	r := &leaseLib.LeaseReq{}
	dec := gob.NewDecoder(bytes.NewBuffer(payload))
	err := dec.Decode(r)
	return *r, err
}

func (lso *LeaseServerObject) InitLeaseObject(pso *PumiceDBServer.PmdbServerObject) {
	lso.Pso = pso
	lso.LeaseMap = make(map[uuid.UUID]*leaseLib.LeaseInfo)
	lso.LeaseColmFam = LEASE_COLUMN_FAMILY
	//Register Lease callbacks
	lso.Pso.LeaseAPI = lso
	lso.listObj = list.New()
	//Start the garbage collector thread
	go lso.leaseGarbageCollector()
}

func (lso *LeaseServerObject) GetLeaderTimeStamp(ts *leaseLib.LeaderTS) error {
	var plts PumiceDBServer.PmdbLeaderTS
	rc := PumiceDBServer.PmdbGetLeaderTimeStamp(&plts)
	ts.LeaderTerm = plts.Term
	ts.LeaderTime = plts.Time
	return rc
}

func (handler *LeaseServerReqHandler) doRefresh(currentTime leaseLib.LeaderTS) int {
	vdev_lease_info, isPresent := handler.LeaseServerObj.LeaseMap[handler.LeaseReq.Resource]
        if (isPresent) {
        	if isPermitted(&vdev_lease_info.LeaseMetaInfo, handler.LeaseReq.Client, currentTime, handler.LeaseReq.Operation) {
                	if (vdev_lease_info.LeaseMetaInfo.LeaseState == leaseLib.EXPIRED) {
                        	// If refresh happens on expired lease, convert the refresh
                                //into raft write.
                                vdev_lease_info.LeaseMetaInfo.LeaseState = leaseLib.INPROGRESS
				return CONTINUE_WR
                         } else {
                                vdev_lease_info.LeaseMetaInfo.LeaseState = leaseLib.GRANTED
                                //Refresh the lease
                                vdev_lease_info.LeaseMetaInfo.TimeStamp = currentTime
                                vdev_lease_info.LeaseMetaInfo.TTL = ttlDefault
                                //Copy the encoded result in replyBuffer
                                copyToResponse(&vdev_lease_info.LeaseMetaInfo, handler.LeaseRes)
				if (handler.LeaseServerObj.listObj.Back() != nil) {
					listOrderSanityCheck(handler.LeaseServerObj.listObj.Back().Value.(*leaseLib.LeaseInfo), vdev_lease_info)
				}
				//Update lease list
                                handler.LeaseServerObj.listObj.MoveToBack(vdev_lease_info.ListElement)
                                //Response from prepare itself
				return SEND_RESPONSE
			}
                 }
        }
	return ERROR
}

func (lso *LeaseServerObject) prepare(request leaseLib.LeaseReq, response *leaseLib.LeaseRes) int {
	var ct leaseLib.LeaderTS
	lso.GetLeaderTimeStamp(&ct)
	

	switch request.Operation {
		case leaseLib.STALE_REMOVAL:
			if request.InitiatorTerm == ct.LeaderTerm {
                        	log.Info("Request for Stale lease processing from same term")
                        	return CONTINUE_WR
                	}
                	log.Info("GC request from previous term encountered")
                	return ERROR

		case leaseLib.REFRESH:

			leaseReq := LeaseServerReqHandler{
                		LeaseServerObj: lso,
                		LeaseReq:       request,
                		LeaseRes:       response,
        		}
			return leaseReq.doRefresh(ct)

		case leaseLib.GET:
			vdev_lease_info, isPresent := lso.LeaseMap[request.Resource]
                	if isPresent {
                        	if !isPermitted(&vdev_lease_info.LeaseMetaInfo, request.Client,
                                	ct, request.Operation) {
                                	//Dont provide lease
                                	return ERROR
                        	}
                	}
                	var li leaseLib.LeaseInfo
                	//Insert or update into MAP
                	li.LeaseMetaInfo.Resource = request.Resource
                	li.LeaseMetaInfo.Client = request.Client
                	li.LeaseMetaInfo.LeaseState = leaseLib.INPROGRESS
                	lso.LeaseMap[request.Resource] = &li
			return CONTINUE_WR
		default:
			log.Error("Invalid operation", request.Operation)
                	return ERROR
	}

	return ERROR

}

func (lso *LeaseServerObject) WritePrep(wrPrepArgs *PumiceDBServer.PmdbCbArgs) int64 {
	var e error
	var ret int64

	//Decode request
	rq, err := Decode(wrPrepArgs.Payload)
	if err != nil {
		log.Error(err)
		return -1
	}

	var rs leaseLib.LeaseRes
	lso.listLock.Lock()
	rc := lso.prepare(rq, &rs)
	lso.listLock.Unlock()

	switch (rc) {
		case ERROR:
			_, e = lso.Pso.CopyDataToBuffer(byte(0), wrPrepArgs.ContinueWr)
                	if e != nil {
                        	log.Error("Failed to Copy result in the buffer: %s", e)
                	}
			return -1

		case SEND_RESPONSE:
			_, e = lso.Pso.CopyDataToBuffer(byte(0), wrPrepArgs.ContinueWr)
                	if e != nil {
                        	log.Error("Failed to Copy result in the buffer: %s", e)
                        	return 0
                	}

			ret, e = lso.Pso.CopyDataToBuffer(rs, wrPrepArgs.ReplyBuf)
                        if e != nil {
                                log.Error("Failed to Copy result in the buffer: %s", e)
                                return 0
                        }
                        return ret

		case CONTINUE_WR:
			_, e = lso.Pso.CopyDataToBuffer(byte(1), wrPrepArgs.ContinueWr)
                	if e != nil {
                        	log.Error("Failed to Copy result in the buffer: %s", e)
                        	return 0
                	}
                	return 0
	}
	
	return 0
}

func (handler *LeaseServerReqHandler) readLease() int {
	if handler.LeaseReq.Operation == leaseLib.LOOKUP {
		l, isPresent := handler.LeaseServerObj.LeaseMap[handler.LeaseReq.Resource]
		if !isPresent {
			return -1
		}
		copyToResponse(&l.LeaseMetaInfo, handler.LeaseRes)

	} else if handler.LeaseReq.Operation == leaseLib.LOOKUP_VALIDATE {
		l, isPresent := handler.LeaseServerObj.LeaseMap[handler.LeaseReq.Resource]
		if !isPresent {
			return -1
		}

		if l.LeaseMetaInfo.LeaseState == leaseLib.INPROGRESS {
			return -1
		}

		var currentTime leaseLib.LeaderTS
		handler.LeaseServerObj.GetLeaderTimeStamp(&currentTime)
		checkMajorCorrectness(currentTime.LeaderTerm, l.LeaseMetaInfo.TimeStamp.LeaderTerm)

		//Take the lock as we are modifying lease parameters here.
		lso := handler.LeaseServerObj
		lso.listLock.Lock()
		if l.LeaseMetaInfo.LeaseState == leaseLib.GRANTED {
			ttl := ttlDefault -
				int(currentTime.LeaderTime - l.LeaseMetaInfo.TimeStamp.LeaderTime)
			if ttl < 0 {
				log.Trace("Lookup validate marking lease as expired locally ",
						l.LeaseMetaInfo.Resource, l.LeaseMetaInfo.Client)
				l.LeaseMetaInfo.TTL = 0
				l.LeaseMetaInfo.LeaseState = leaseLib.EXPIRED_LOCALLY
			} else {
				l.LeaseMetaInfo.TTL = ttl
			}
		}
		lso.listLock.Unlock()

		copyToResponse(&l.LeaseMetaInfo, handler.LeaseRes)
	}
	return 0
}

func (lso *LeaseServerObject) Read(readArgs *PumiceDBServer.PmdbCbArgs) int64 {

	log.Trace("NiovaCtlPlane server: Read request received")

	//Decode the request structure sent by client.
	reqStruct, err := Decode(readArgs.Payload)
	if err != nil {
		log.Error(err)
		return -1
	}

	var returnObj leaseLib.LeaseRes
	var replySize int64
	var copyErr error

	leaseReq := LeaseServerReqHandler{
		LeaseServerObj: lso,
		LeaseReq:       reqStruct,
		LeaseRes:       &returnObj,
	}

	rc := leaseReq.readLease()

	if rc == 0 {
		replySize, copyErr = lso.Pso.CopyDataToBuffer(returnObj, readArgs.ReplyBuf)
		if copyErr != nil {
			log.Error("Failed to Copy result in the buffer: %s", copyErr)
			return -1
		}

		return replySize
	}

	return int64(rc)
}

func listOrderSanityCheck(fe *leaseLib.LeaseInfo, se *leaseLib.LeaseInfo) {
	log.Info("Sanity checks")
	if ((fe != nil) && (se != nil)) {
		exp_fe := fe.LeaseMetaInfo.TimeStamp.LeaderTime
		exp_se := se.LeaseMetaInfo.TimeStamp.LeaderTime
		log.Info("Time ", exp_fe, exp_se)
		if (exp_fe > exp_se) {
			log.Fatal("(Lease server) List ordering issue  detected")
		}
	}
}

func (handler *LeaseServerReqHandler) initLease() (*leaseLib.LeaseInfo, error) {
	var lo leaseLib.LeaseInfo
	var lop *leaseLib.LeaseInfo
	handler.LeaseServerObj.listLock.Lock()
	lop, isPresent := handler.LeaseServerObj.LeaseMap[handler.LeaseReq.Resource]
	if !isPresent {
		lop = &lo
		lop.LeaseMetaInfo.Resource = handler.LeaseReq.Resource
		lop.LeaseMetaInfo.Client = handler.LeaseReq.Client
		handler.LeaseServerObj.LeaseMap[handler.LeaseReq.Resource] = lop
	}
	err := handler.LeaseServerObj.GetLeaderTimeStamp(&lop.LeaseMetaInfo.TimeStamp)
	lop.LeaseMetaInfo.LeaseState = leaseLib.GRANTED
	lop.LeaseMetaInfo.TTL = ttlDefault

	//Insert into list
	lop.ListElement = &list.Element{}
	lop.ListElement.Value = lop

	//Any apply requires lease to be pushed back in the list
	if (handler.LeaseServerObj.listObj.Back() != nil) {
		listOrderSanityCheck(handler.LeaseServerObj.listObj.Back().Value.(*leaseLib.LeaseInfo), lop)
	}
	handler.LeaseServerObj.listObj.PushBack(lop)
	handler.LeaseServerObj.listLock.Unlock()

	return lop, err
}

func (handler *LeaseServerReqHandler) applyLease() int {

	lop, isleader := handler.initLease()

	copyToResponse(&lop.LeaseMetaInfo, handler.LeaseRes)
	valueBytes := bytes.Buffer{}
	enc := gob.NewEncoder(&valueBytes)
	err := enc.Encode(lop.LeaseMetaInfo)
	if err != nil {
		log.Error(err)
		return 1
	}

	byteToStr := string(valueBytes.Bytes())

	// Length of value.
	valLen := len(byteToStr)
	keyLength := len(handler.LeaseReq.Resource.String())
	rc := handler.LeaseServerObj.Pso.WriteKV(handler.UserID, handler.PmdbHandler, handler.LeaseReq.Resource.String(), int64(keyLength), byteToStr, int64(valLen), handler.LeaseServerObj.LeaseColmFam)
	if rc < 0 {
		lop.LeaseMetaInfo.Status = leaseLib.FAILURE
		log.Error("Value not written to rocksdb")
		return -1
	} else {
		lop.LeaseMetaInfo.Status = leaseLib.SUCCESS
	}

	if isleader == nil {
		copyToResponse(&lop.LeaseMetaInfo, handler.LeaseRes)
		return 0
	}

	//Check if lease is added properly
	_, isPresent1 := handler.LeaseServerObj.LeaseMap[handler.LeaseReq.Resource]

	if !isPresent1 {
		log.Info("Just added lease but not present in map!")
	}
	return 1
}

func (handler *LeaseServerReqHandler) setLeaseExpired(resource uuid.UUID) {
	lso := handler.LeaseServerObj

	lso.listLock.Lock()
	lease := handler.LeaseServerObj.LeaseMap[resource]
	lease.LeaseMetaInfo.LeaseState = leaseLib.EXPIRED
	lease.LeaseMetaInfo.TTL = 0

	//Remove from list
	handler.LeaseServerObj.listObj.Remove(lease.ListElement)

	log.Trace("Marking lease expired: ", lease.LeaseMetaInfo.Resource, lease.LeaseMetaInfo.Client)
	lso.listLock.Unlock()

}

func (handler *LeaseServerReqHandler) gcReqHandler() {
	for i := 0; i < len(handler.LeaseReq.Resources); i++ {
		//Set lease as expired
		resource := handler.LeaseReq.Resources[i]
		handler.setLeaseExpired(resource)
		lease := handler.LeaseServerObj.LeaseMap[resource]
		//Write to RocksDB
		valueBytes := bytes.Buffer{}
		enc := gob.NewEncoder(&valueBytes)
		err := enc.Encode(lease.LeaseMetaInfo)
		if err != nil {
			log.Error(err)
			break
		}
		byteToStr := string(valueBytes.Bytes())
		valLen := len(byteToStr)

		handler.LeaseServerObj.Pso.WriteKV(handler.UserID, handler.PmdbHandler,
			resource.String(), int64(len(resource.String())), byteToStr, int64(valLen),
			handler.LeaseServerObj.LeaseColmFam)
	}
}

func (lso *LeaseServerObject) Apply(applyArgs *PumiceDBServer.PmdbCbArgs) int64 {
	//var valueBytes bytes.Buffer
	var copyErr error
	var replySizeRc int64

	// Decode the input buffer into structure format
	applyLeaseReq, err := Decode(applyArgs.Payload)
	if err != nil {
		log.Error(err)
		return -1
	}

	log.Trace("Apply for operation: ", applyLeaseReq.Operation)

	//Handle client request
	var returnObj leaseLib.LeaseRes
	leaseReq := LeaseServerReqHandler{
		LeaseServerObj: lso,
		LeaseReq:       applyLeaseReq,
		LeaseRes:       &returnObj,
		UserID:         applyArgs.UserID,
		PmdbHandler:    applyArgs.PmdbHandler,
	}

	//Handle GC request
	if applyLeaseReq.Operation == leaseLib.STALE_REMOVAL {
		leaseReq.gcReqHandler()
		return 0
	}

	//Handler GET lease request
	rc := leaseReq.applyLease()
	log.Trace("(Apply) Lease request by client : ",
				applyLeaseReq.Client.String(), " for resource : ",
				applyLeaseReq.Resource.String())

	//Copy the encoded result in replyBuffer
	replySizeRc = 0
	if rc == 0 && applyArgs.ReplyBuf != nil {
		replySizeRc, copyErr = lso.Pso.CopyDataToBuffer(returnObj,
														applyArgs.ReplyBuf)
		if copyErr != nil {
			log.Error("Failed to Copy result in the buffer: %s", copyErr)
			return -1
		}
	} else {
		return int64(rc)
	}
	return replySizeRc
}

func (lso *LeaseServerObject) leaderInit() {
	for _, lo := range lso.LeaseMap {
		if (lo.LeaseMetaInfo.LeaseState != leaseLib.EXPIRED) {
			rc := lso.GetLeaderTimeStamp(&lo.LeaseMetaInfo.TimeStamp)
			if rc != nil {
				log.Error("Unable to get timestamp (InitLeader)")
			}
			lo.LeaseMetaInfo.TTL = ttlDefault
			lo.LeaseMetaInfo.LeaseState = leaseLib.GRANTED
		}
	}
}

func (lso *LeaseServerObject) peerBootup(userID unsafe.Pointer) {
	readResult, _, _ := lso.Pso.ReadAllKV(userID, "", 0, 0, lso.LeaseColmFam)

	//Result of the read
	for key, value := range readResult {
		//Decode the request structure sent by client.
		var leaseInfo leaseLib.LeaseInfo
		dec := gob.NewDecoder(bytes.NewBuffer(value))
		decodeErr := dec.Decode(&leaseInfo.LeaseMetaInfo)
		if decodeErr != nil {
			log.Error("Failed to decode the read request : ", decodeErr)
			return
		}
		kuuid, _ := uuid.FromString(key)
		lso.LeaseMap[kuuid] = &leaseInfo
		leaseInfo.ListElement = &list.Element{}
		leaseInfo.ListElement.Value = &leaseInfo
		lso.listObj.PushBack(&leaseInfo)
		delete(readResult, key)
	}
}

func (lso *LeaseServerObject) leaseGarbageCollector() {
	//Init ticker
	t := time.NewTicker(time.Duration(gcTimeout) * time.Second)
	log.Info("GC routine running")
	for {
		<-t.C
		var ct leaseLib.LeaderTS
		err := lso.GetLeaderTimeStamp(&ct)
		if err != nil {
			continue
		}
		var rUUIDs []uuid.UUID
		stopScan := false
		//Iterate over list
		for e := lso.listObj.Front(); e != nil; e = e.Next() {
			if cobj, ok := e.Value.(*leaseLib.LeaseInfo); ok {
				//Check lease status before processing it.
				obj := cobj.LeaseMetaInfo
				if obj.LeaseState != leaseLib.GRANTED &&
					obj.LeaseState != leaseLib.STALE_INPROGRESS &&
					obj.LeaseState != leaseLib.EXPIRED_LOCALLY {
					continue
				}
				err = lso.GetLeaderTimeStamp(&ct)
				if err != nil {
					//Leader change, so stop the routine execution
					break
				}

				//Obtain lock
				lso.listLock.Lock()
				lt := obj.TimeStamp.LeaderTime
				ttl := ttlDefault - int(ct.LeaderTime - lt)
				if ttl <= 0 {
					cobj.LeaseMetaInfo.LeaseState = leaseLib.STALE_INPROGRESS
					log.Trace("Enqueue lease for stale lease processing: ",
							cobj.LeaseMetaInfo.Resource, cobj.LeaseMetaInfo.Client)
					rUUIDs = append(rUUIDs, obj.Resource)
				} else {
					log.Trace("GC scanning finished")
					stopScan = true
				}
				lso.listLock.Unlock()
			}
			//no more expired leases.
			if stopScan {
				break;
			}
		}
		//Collect till expired

		if len(rUUIDs) > 0 {
			//Create request
			var r leaseLib.LeaseReq
			r.Operation = leaseLib.STALE_REMOVAL
			r.Resources = rUUIDs
			r.InitiatorTerm = ct.LeaderTerm
			//Send Request
			log.Trace("Send stale lease processing request")
			err := PumiceDBServer.PmdbEnqueueDirectWriteRequest(r)
			if err != nil {
				log.Error("Failed to send stale lease processing request")
			}
		}
	}
}

func (lso *LeaseServerObject) Init(initPeerArgs *PumiceDBServer.PmdbCbArgs) {
	if initPeerArgs.InitState == PumiceDBServer.INIT_BECOMING_LEADER_STATE {
		log.Info("Init callback in peer becoming leader.")
		lso.leaderInit()
	} else if initPeerArgs.InitState == PumiceDBServer.INIT_BOOTUP_STATE {
		log.Info("Init callback on peer bootup.")
		lso.peerBootup(initPeerArgs.UserID)
	} else if initPeerArgs.InitState == PumiceDBServer.INIT_BECOMING_CANDIDATE_STATE {
		log.Info("Init callback on peer becoming candidate.")
	} else {
		log.Error("Invalid init state: %d", initPeerArgs.InitState)
	}
}
