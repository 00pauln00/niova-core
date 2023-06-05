package leaseServer

import (
	"bytes"
	leaseLib "common/leaseLib"
	list "container/list"
	"encoding/gob"
	uuid "github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
	PumiceDBServer "niova/go-pumicedb-lib/server"
	"sync"
	"time"
	"unsafe"
	"errors"
)

var ttlDefault = 60
var gcTimeout = 35
const MAX_SINGLE_GC_REQ = 100
var LEASE_COLUMN_FAMILY = "NIOVA_LEASE_CF"

const (
	//Go return codes
	ERROR         int = -1
	SEND_RESPONSE     = 0
	CONTINUE_WR       = 1

	//List operations
	PUSH = 0
	MOVE_TO_BACK = 1
	REMOVE = 2
)

type LeaseServerObject struct {
	LeaseColmFam string
	LeaseMap     map[uuid.UUID]*leaseLib.LeaseInfo
	Pso          *PumiceDBServer.PmdbServerObject
	listObj      *list.List
	leaseLock    sync.RWMutex
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

func (lso *LeaseServerObject) isExpired(ts leaseLib.LeaderTS) bool {
	var ct leaseLib.LeaderTS
	lso.GetLeaderTimeStamp(&ct)
	checkMajorCorrectness(ct.LeaderTerm, ts.LeaderTerm)
	//Check if expired
	if ts.LeaderTime+int64(ttlDefault) <= ct.LeaderTime {
		return true
	}
	return false
}

func (lso *LeaseServerObject) isPermitted(entry *leaseLib.LeaseMeta, clientUUID uuid.UUID, operation int) bool {
	//Check if provided lease operation is permitted for the current lease state
	switch entry.LeaseState {
	case leaseLib.INPROGRESS:
		fallthrough
	case leaseLib.STALE_INPROGRESS:
		return false
	case leaseLib.EXPIRED:
		//Allow GET from any client
                if (operation == leaseLib.GET) {
                        return true
                }
                //Reject REFRESH from any client
                return false
	case leaseLib.GRANTED:
		//Allow REFRESH from the same client
		if (operation == leaseLib.REFRESH) && (clientUUID == entry.Client) {
                        return true
                }
		//Reject any other request
                return false
	}
	return false
}

func Decode(payload []byte) (leaseLib.LeaseReq, error) {
	r := &leaseLib.LeaseReq{}
	dec := gob.NewDecoder(bytes.NewBuffer(payload))
	err := dec.Decode(r)
	return *r, err
}


func (lso *LeaseServerObject) listOperation(element *leaseLib.LeaseInfo, opcode int, sanityCheck bool) {
	//Sanity check
	if ((sanityCheck) && (lso.listObj.Len() > 0)) {
		listOrderSanityCheck(lso.listObj.Back().Value.(*leaseLib.LeaseInfo), element)
        }
	
	switch opcode { 
	case PUSH:	
		lso.listObj.PushBack(element)
	case MOVE_TO_BACK:
		lso.listObj.MoveToBack(element.ListElement)
	case REMOVE:
		lso.listObj.Remove(element.ListElement)
	}

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
	if ts == nil {
		return errors.New("Parameter is nil")
	}
	var plts PumiceDBServer.PmdbLeaderTS
	rc := PumiceDBServer.PmdbGetLeaderTimeStamp(&plts)
	if (rc == nil) {
		ts.LeaderTerm = plts.Term
		ts.LeaderTime = plts.Time
	}
	return rc
}

func (handler *LeaseServerReqHandler) doRefresh() int {

	l, isPresent := handler.LeaseServerObj.LeaseMap[handler.LeaseReq.Resource]
	if (!isPresent) || (isPresent && (handler.LeaseReq.Client != l.LeaseMetaInfo.Client)) {
		return ERROR
	}

	switch l.LeaseMetaInfo.LeaseState {
	case leaseLib.INPROGRESS:
		fallthrough
	
	case leaseLib.STALE_INPROGRESS:
		return ERROR

	case leaseLib.EXPIRED:
		l.LeaseMetaInfo.LeaseState = leaseLib.INPROGRESS
		return CONTINUE_WR

	case leaseLib.EXPIRED_LOCALLY:
		fallthrough

	case leaseLib.GRANTED:
		//Refresh the lease
		l.LeaseMetaInfo.LeaseState = leaseLib.GRANTED
		var currentTime leaseLib.LeaderTS
		err := handler.LeaseServerObj.GetLeaderTimeStamp(&currentTime)
		if err != nil {
			return ERROR
		}
		
		l.LeaseMetaInfo.TimeStamp = currentTime
		l.LeaseMetaInfo.TTL = ttlDefault

		//Copy the encoded result in replyBuffer
		copyToResponse(&l.LeaseMetaInfo, handler.LeaseRes)

		//Update lease list
		handler.LeaseServerObj.listOperation(l, MOVE_TO_BACK, true)
		//Response from prepare itself
		return SEND_RESPONSE
	default:
		log.Error("Unexpected lease state encountered in refresh : ", l.LeaseMetaInfo.LeaseState)
	}
	return ERROR
}

func (lso *LeaseServerObject) prepare(request leaseLib.LeaseReq, response *leaseLib.LeaseRes) int {
	var ct leaseLib.LeaderTS
	lso.GetLeaderTimeStamp(&ct)

	//Obtain lock
	lso.leaseLock.Lock()
	defer lso.leaseLock.Unlock()

	//Switch
	switch request.Operation {
	case leaseLib.STALE_REMOVAL:
		if request.InitiatorTerm == ct.LeaderTerm {
			log.Trace("Request for Stale lease processing from same term")
			return CONTINUE_WR
		}
		log.Trace("GC request from previous term encountered")
		return ERROR

	case leaseLib.REFRESH:

		leaseReq := LeaseServerReqHandler{
			LeaseServerObj: lso,
			LeaseReq:       request,
			LeaseRes:       response,
		}
		return leaseReq.doRefresh()

	case leaseLib.GET:
		vdev_lease_info, isPresent := lso.LeaseMap[request.Resource]
		if (isPresent) && (!lso.isPermitted(&vdev_lease_info.LeaseMetaInfo, request.Client, request.Operation)) {
			//Dont provide lease
			return ERROR
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
	rc := lso.prepare(rq, &rs)

	switch rc {
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
	handler.LeaseServerObj.leaseLock.Lock()
	defer handler.LeaseServerObj.leaseLock.Unlock()

	l, isPresent := handler.LeaseServerObj.LeaseMap[handler.LeaseReq.Resource]
        if (!isPresent) || (isPresent && l.LeaseMetaInfo.LeaseState == leaseLib.INPROGRESS) {
        	return ERROR
        }

	switch handler.LeaseReq.Operation {
	case leaseLib.LOOKUP:
		break

	case leaseLib.LOOKUP_VALIDATE:
		lso := handler.LeaseServerObj
		if l.LeaseMetaInfo.LeaseState == leaseLib.GRANTED {
			if handler.LeaseServerObj.isExpired(l.LeaseMetaInfo.TimeStamp) {
				l.LeaseMetaInfo.TTL = 0
				l.LeaseMetaInfo.LeaseState = leaseLib.EXPIRED_LOCALLY
			} else {
				var ct leaseLib.LeaderTS
				lso.GetLeaderTimeStamp(&ct)
				l.LeaseMetaInfo.TTL = int(l.LeaseMetaInfo.TimeStamp.LeaderTime - ct.LeaderTime + int64(l.LeaseMetaInfo.TTL))
			}
		}

	default:
		log.Error("Unkown read lease operation ", handler.LeaseReq.Operation)
		return ERROR
	}

	copyToResponse(&l.LeaseMetaInfo, handler.LeaseRes)
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

	//Prepare request obj
	leaseReq := LeaseServerReqHandler{
		LeaseServerObj: lso,
		LeaseReq:       reqStruct,
		LeaseRes:       &returnObj,
	}
	//Read lease
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
	log.Trace("Sanity checks for list order")
	if (fe != nil) && (se != nil) {
		exp_fe := fe.LeaseMetaInfo.TimeStamp.LeaderTime
		exp_se := se.LeaseMetaInfo.TimeStamp.LeaderTime
		if exp_fe > exp_se {
			log.Fatal("(Lease server) List ordering issue  detected")
		}
	}
}

func (handler *LeaseServerReqHandler) initLease() (*leaseLib.LeaseInfo, error) {
	var lo leaseLib.LeaseInfo
	var lop *leaseLib.LeaseInfo
	handler.LeaseServerObj.leaseLock.Lock()
	
	//Check if resource is already present in the map, if not create new entry
	lop, isPresent := handler.LeaseServerObj.LeaseMap[handler.LeaseReq.Resource]
	if !isPresent {
		lop = &lo
		lop.LeaseMetaInfo.Resource = handler.LeaseReq.Resource
		lop.LeaseMetaInfo.Client = handler.LeaseReq.Client
		handler.LeaseServerObj.LeaseMap[handler.LeaseReq.Resource] = lop
	} 

	err := handler.LeaseServerObj.GetLeaderTimeStamp(&lop.LeaseMetaInfo.TimeStamp)
	//Check if lease state was set to inprogress in leader
        if (err == nil) && (lop.LeaseMetaInfo.LeaseState != leaseLib.INPROGRESS) {
		log.Fatal("Wrong state transition identified in GET lease, expected INPROGRESS, identified : ", lop.LeaseMetaInfo.LeaseState)
        }

	lop.LeaseMetaInfo.LeaseState = leaseLib.GRANTED
	lop.LeaseMetaInfo.TTL = ttlDefault

	//Insert into list
	lop.ListElement = &list.Element{}
	lop.ListElement.Value = lop

	//Any apply requires lease to be pushed back in the list
	//Do list sanity check only if its a leader
	handler.LeaseServerObj.listOperation(lop, PUSH, err == nil)
	handler.LeaseServerObj.leaseLock.Unlock()

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
	return 1
}

func (handler *LeaseServerReqHandler) gcReqHandler() {
	for i := 0; i < handler.LeaseReq.LeaseCount; i++ {
		//Set lease as expired
		resource := handler.LeaseReq.Resources[i]
		isLeader := PumiceDBServer.PmdbIsLeader()
		
		//Obtain lock
		handler.LeaseServerObj.leaseLock.Lock()

		//Update lease in map
		lease, isPresent := handler.LeaseServerObj.LeaseMap[resource]
		if (!isPresent) || (isLeader && isPresent && lease.LeaseMetaInfo.LeaseState != leaseLib.STALE_INPROGRESS) {
			log.Error("GCed resource is not present or have modified state : ", resource)
			handler.LeaseServerObj.leaseLock.Unlock()
			continue
		}

		//Set lease as expired
		lease.LeaseMetaInfo.LeaseState = leaseLib.EXPIRED
		lease.LeaseMetaInfo.TTL = 0

		//Remove from list
		handler.LeaseServerObj.listOperation(lease, REMOVE, false)

		//Write to RocksDB
		valueBytes := bytes.Buffer{}
		enc := gob.NewEncoder(&valueBytes)
		err := enc.Encode(lease.LeaseMetaInfo)
		if err != nil {
			log.Error(err)
			handler.LeaseServerObj.leaseLock.Unlock()
			continue
		}
		handler.LeaseServerObj.leaseLock.Unlock()

		byteToStr := string(valueBytes.Bytes())
		valLen := len(byteToStr)
		rc := handler.LeaseServerObj.Pso.WriteKV(handler.UserID, handler.PmdbHandler,
			resource.String(), int64(len(resource.String())), byteToStr, int64(valLen),
			handler.LeaseServerObj.LeaseColmFam)
		if rc < 0 {
			log.Error("Expired lease update to RocksDB failed")
		}
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
	switch applyLeaseReq.Operation {
	case leaseLib.STALE_REMOVAL:
		leaseReq.gcReqHandler()
		return 0
	case leaseLib.REFRESH:
		fallthrough
	case leaseLib.GET:
		rc := leaseReq.applyLease()
		if !((rc == 0) && (applyArgs.ReplyBuf != nil)) {
			return int64(rc)
		}
		replySizeRc, copyErr = lso.Pso.CopyDataToBuffer(returnObj,
			applyArgs.ReplyBuf)
			if copyErr != nil {
			log.Error("Failed to Copy result in the buffer: %s", copyErr)
			return int64(ERROR)
		}
		return replySizeRc
	default:
		log.Error("Unkown lease operation identified in Apply callback : ", applyLeaseReq.Operation)
	}

	return int64(ERROR)
}

func (lso *LeaseServerObject) leaderInit() {
	for e := lso.listObj.Front(); e != nil; e = e.Next() {
		if lo, ok := e.Value.(*leaseLib.LeaseInfo); ok {
			if lo.LeaseMetaInfo.LeaseState != leaseLib.EXPIRED {
				rc := lso.GetLeaderTimeStamp(&lo.LeaseMetaInfo.TimeStamp)
				if rc != nil {
					log.Error("Unable to get timestamp (InitLeader)")
				}
				lo.LeaseMetaInfo.TTL = ttlDefault
				lo.LeaseMetaInfo.LeaseState = leaseLib.GRANTED
			} else {
				log.Error("Leader marked as EXPIRED and present in the list")
			}
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
		if leaseInfo.LeaseMetaInfo.LeaseState != leaseLib.EXPIRED {
			leaseInfo.ListElement = &list.Element{}
			leaseInfo.ListElement.Value = &leaseInfo
			lso.listOperation(&leaseInfo, PUSH, false)
		}
		delete(readResult, key)
	}
}

func (lso *LeaseServerObject) sendGCReq(resourceUUIDs [MAX_SINGLE_GC_REQ]uuid.UUID, leaseCount int, leaderTerm int64) int {
	//Send GC request if only expired lease found
	if leaseCount == 0 {
		return 0
	}
	var r leaseLib.LeaseReq
	r.Operation = leaseLib.STALE_REMOVAL
	r.InitiatorTerm = leaderTerm
	r.Resources = resourceUUIDs[:]
	r.LeaseCount = leaseCount

	//Send Request
	log.Trace("Send stale lease processing request")
	err := PumiceDBServer.PmdbEnqueueDirectWriteRequest(r)
	return err
}


func (lso *LeaseServerObject) leaseGarbageCollector() {
	//Init ticker
	//Sleep default at init
	sleep := int64(ttlDefault)
	log.Info("GC routine running")
	for {
		time.Sleep(time.Duration(sleep) * time.Second)
		var ct leaseLib.LeaderTS
		err := lso.GetLeaderTimeStamp(&ct)
		if err != nil {
			//Reset the sleep
			sleep = int64(ttlDefault)
			continue
		}

		var rUUIDs [MAX_SINGLE_GC_REQ]uuid.UUID
		stopScan := false
		index := 0
		//Obtain lock
		lso.leaseLock.Lock()
		//Iterate over list
		for e := lso.listObj.Front(); e != nil; e = e.Next() {
			if cobj, ok := e.Value.(*leaseLib.LeaseInfo); ok {
				//Check lease status before processing it.
				obj := cobj.LeaseMetaInfo

				//We dont process lease which are marked as STALE_INPROGRESS
				//and if StaleRetry is set to false
				if (!obj.StaleRetry && obj.LeaseState == leaseLib.STALE_INPROGRESS) {
					continue
				} 
				

				cobj.LeaseMetaInfo.StaleRetry = false
				if lso.isExpired(obj.TimeStamp) {
					cobj.LeaseMetaInfo.LeaseState = leaseLib.STALE_INPROGRESS
					log.Trace("Enqueue lease for stale lease processing: ",
						cobj.LeaseMetaInfo.Resource, cobj.LeaseMetaInfo.Client)
					rUUIDs[index] = obj.Resource
					//Break if max resource in a request is reached
					index += 1
					if index == MAX_SINGLE_GC_REQ {
						stopScan = true
					}

				} else {
					log.Trace("GC scanning finished")
					stopScan = true
				}
			}
			//no more expired leases.
			if stopScan {
				if e.Next() != nil {
					nextLease := e.Value.(*leaseLib.LeaseInfo).LeaseMetaInfo.TimeStamp.LeaderTime
                        		var ct leaseLib.LeaderTS
                        		lso.GetLeaderTimeStamp(&ct)
                        		sleep = nextLease + int64(ttlDefault) - ct.LeaderTime
					//Avoid negative value for sleep
					if sleep < 0 {
						sleep = 0
					}

				} else {
					sleep = int64(ttlDefault)
				}
				break
			}
		}

		//Release lock
		lso.leaseLock.Unlock()

		//Send GC request if only expired lease found
		rc := lso.sendGCReq(rUUIDs, index, ct.LeaderTerm)
		switch rc {
        	case 0:
                	log.Trace("GC request successful")
        	case -11:
                	log.Error("Retry stale lease request")
                	lso.leaseLock.Lock()
                	for i := 0; i < index; i++ {
                        	l, isPresent := lso.LeaseMap[rUUIDs[i]]
                        	if (!isPresent) || (isPresent && l.LeaseMetaInfo.LeaseState != leaseLib.STALE_INPROGRESS) {
                                	log.Error("(sendGCReq) GCed resource is not present or have modified state", rUUIDs[i])
                                	continue
                        	}
                        	l.LeaseMetaInfo.StaleRetry = true
                	}
                	lso.leaseLock.Unlock()
                	sleep = 0
        	default:
                	log.Error("Failed to send stale lease processing request : ", err)

        	}
	}
}

func (lso *LeaseServerObject) Init(initPeerArgs *PumiceDBServer.PmdbCbArgs) {
	switch initPeerArgs.InitState {
	case PumiceDBServer.INIT_BECOMING_LEADER_STATE:
		log.Info("Init callback in peer becoming leader.")
		lso.leaderInit()
	case PumiceDBServer.INIT_BOOTUP_STATE:
		log.Info("Init callback on peer bootup.")
		lso.peerBootup(initPeerArgs.UserID)
	case PumiceDBServer.INIT_BECOMING_CANDIDATE_STATE:
		log.Info("Init callback on peer becoming candidate.")
	default:
		log.Error("Invalid init state: %d", initPeerArgs.InitState)
	}
}
