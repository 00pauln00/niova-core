
import argparse
from pickle import TRUE
from pickletools import read_uint1
from platform import java_ver
from sys import api_version
from unicodedata import ucd_3_2_0
from tqdm import tqdm
import re
import csv

class request:
        def __init__(self,servercount):
                self.followerreqrec=[None]*servercount
                self.followersync=[None]*servercount
                self.acks=[None]*servercount
                self.serverstatus=[None]*servercount
                self.followersyncend=[None]*servercount
                self.uofsync=[None]*servercount
                self.multtry=False
        def setnum(self,num):
                self.num=int(num)
        def gettype(self):
                return self.optype
        def settype(self,optype):
                self.optype=optype 
        def setcreatereqtime(self,createreq):
                self.createreq=createreq
        def setclientreqsendtime(self,clientreqsend):
                self.clientreqsend=clientreqsend
        def setleaderreqrectime(self,leaderreqrec):
                self.leaderreqrec=leaderreqrec
        def settimeouttime(self,timeout):
                self.timeout=timeout
        def setrelatedentries(self,entry):
                self.relentry=entry
        def getrelatedentries(self):
                return self.relentry
        def setsendfollowerstime(self,sendfollowers):
                self.sendfollowers=sendfollowers
        def setfollowerreqrectime(self,followerreqrec,cluster,uuid):
                i=0
                for ser in cluster:
                        if ser.uuid == uuid:
                                self.followerreqrec[i]=followerreqrec
                        i=i+1
        def setsynctime(self,followersync,u,cluster,uuid):
                i=0
                for ser in cluster:
                        if ser.uuid == uuid:
                                self.followersync[i]=followersync
                                self.uofsync[i]=u
                        i=i+1
        def setsyncendtime(self,followersyncend,cluster,uuid):
                i=0
                for ser in cluster:
                        if ser.uuid == uuid:
                                self.followersyncend[i]=followersyncend
                        i=i+1
        def setackstime(self,acks,cluster,uuid):
                i=0
                for ser in cluster:
                        if ser.uuid == uuid:
                                self.acks[i]=acks
                        i=i+1
        def setreptoclitime(self,reptocli):
                self.reptocli=reptocli
        def setclientreqrectime(self,clientreqrec):
                self.clientreqrec=clientreqrec
        def setserverstatus(self,cluster,uuid,status):
                i=0
                for ser in cluster:
                        if ser.uuid == uuid:
                                self.serverstatus[i]=status
                        i=i+1
        def setlookup(self,lookup):
                self.lookup=lookup
        def setread(self,read):
                self.read=read
        def setreadcomplete(self,readcomplete):
                self.readcomplete=readcomplete
        def getdata(self):
                times=[]
                createtime=float(self.createreq)
                endtime=float(self.clientreqrec)
                times.append(self.getid())
                times.append(endtime-createtime)
                times.append(self.num)
                times.append(createtime)
                times.append('{0:.18f}'.format(float(self.clientreqsend)-createtime))
                times.append('{0:.18f}'.format(float(self.leaderreqrec)-float(self.clientreqsend)))
                if self.optype == "write":
                        times.append('{0:.18f}'.format(float(self.timeout)-float(self.leaderreqrec)))
                        times.append('{0:.18f}'.format(float(self.sendfollowers)-float(self.timeout)))
                        for recieved in self.followerreqrec:
                                if recieved==None:
                                        times.append(0)
                                else:
                                        times.append('{0:.18f}'.format(float(recieved)-float(self.sendfollowers)))
                        for i in range(len(self.followersync)):
                                if self.followerreqrec[i]==None:
                                        times.append('{0:.18f}'.format(float(self.followersync[i])-float(self.timeout)))
                                else:
                                        times.append('{0:.18f}'.format(float(self.followersync[i])-float(self.followerreqrec[i])))
                        for i in range(len(self.followersyncend)):
                                times.append('{0:.18f}'.format(float(self.followersyncend[i])-float(self.followersync[i])))
                        lastack=0
                        for i in range(len(self.acks)):
                                if self.acks[i]==None:
                                        times.append(0)
                                else:
                                        times.append('{0:.18f}'.format(float(self.acks[i])-float(self.followersyncend[i])))
                                        if float(self.acks[i])>lastack:
                                                lastack=float(self.acks[i])
                        times.append('{0:.18f}'.format(float(self.reptocli)-lastack))
                else:
                        times.append('{0:.18f}'.format(float(self.lookup)-float(self.leaderreqrec)))
                        times.append('{0:.18f}'.format(float(self.read)-float(self.lookup)))
                        times.append('{0:.18f}'.format(float(self.readcomplete)-float(self.read)))
                        times.append('{0:.18f}'.format(float(self.reptocli)-float(self.readcomplete)))
                times.append('{0:.18f}'.format(float(self.clientreqrec)-float(self.reptocli)))
                for status in self.serverstatus:
                        times.append(status)
                if self.multtry:
                        times.append("TRUE")
                return times
        def tookmulttries(self):
                self.multtry=True
        def setid(self,id):
                self.id=id
        def getid(self):
                return self.id
        def setcrc(self,crc):
                self.crc=crc
        def getcrc(self):
                return self.crc
        def getnum(self):
                return self.num
class server:        
        def __init__(self,uuid):
                self.uuid=uuid
                self.leaderrecieve=[]
                self.candidate=[]
                self.timeouts=[]
                self.sendtofol=[]
                self.folrecreq=[]
                self.startsync=[]
                self.endsync=[]
                self.leadackfol=[]
                self.reptocli=[]
                self.lookup=[]
                self.read=[]
                self.readcomplete=[]
        def addleaderrecieve(self,line):
            self.leaderrecieve.append(line)
        def addcandidate(self,line):
            self.candidate.append(line)
        def addtimeouts(self,line):
            self.timeouts.append(line)
        def addsendtofol(self,line):
            self.sendtofol.append(line)
        def addfolrecreq(self,line):
            self.folrecreq.append(line)
        def addstartsync(self,line):
            self.startsync.append(line)
        def addendsync(self,line):
            self.endsync.append(line)
        def addleadackfol(self,line):
            self.leadackfol.append(line)
        def addreptocli(self,line):
            self.reptocli.append(line)
        def addlookup(self,line):
            self.lookup.append(line)
        def addread(self,line):
            self.read.append(line)
        def addreadcomplete(self,line):
            self.readcomplete.append(line)
class client:
        def __init__(self):
            self.creates=[]
            self.sendtolead=[]
            self.opcomplete=[]
            self.leadredirect=[]
        def addcreate(self,line):
            self.creates.append(line)
        def addsendtolead(self,line):
            self.sendtolead.append(line)
        def addcompletedop(self,line):
            self.opcomplete.append(line)
        def addleadredirect(self,line):
            self.leadredirect.append(line)
class tracker:
        def __init__(self,line):
                self.uses=1
                self.line=line
        def inc_uses(self):
                self.uses+=1
        def get_uses(self):
                return self.uses
        def comp_line(self,ln):
                return self.line==ln
def makewritelabels(cluster):
        labels=["msg id","total time","request number","request created","request sent to leader","leader receives request","coalesce wait time","send to followers"]
        for serv in cluster:
                lab=serv.uuid,"received request"
                labels.append(lab)
        for serv in cluster:
                lab=serv.uuid,"sync wait"
                labels.append(lab)
        for serv in cluster:
                lab=serv.uuid,"sync end"
                labels.append(lab)
        for serv in cluster:
                lab=serv.uuid,"acknowledged"
                labels.append(lab)
        labels.append("leader sends reply")
        labels.append("client receives reply")
        for serv in cluster:
                lab=serv.uuid,"status"
                labels.append(lab)
        labels.append("Multiple Attempts")
        return labels
def makereadlabels(cluster):
        labels=["msg id","total time","request number","request created","request sent to leader","leader receives request","pmdb_sm_handler_client_rw_op@810","read","read complete","leader sends reply","client receives reply"]
        for serv in cluster:
                lab=serv.uuid,"status"
                labels.append(lab)
        labels.append("Multiple Attempts")
        return labels
def grabtime(line):
        sl=line.split(':')
        t=sl[0]
        return t[1:]
def remove_item(list, item):
        newarr = [i for i in list if item not in i]
        return newarr

parser = argparse.ArgumentParser()
parser.add_argument("-cf", "--clientfile",type=str, help="the client file")
parser.add_argument("-sf", "--serverfiles",type=str, help="the server file names seperated by commas")
parser.add_argument("-o", "--output", type=str,help="Out Put File location/name")
args = parser.parse_args()

sf = args.serverfiles.split(',')
file = open(args.clientfile, "r")
clilines=file.readlines()
clilog=client()
for i in tqdm (range(len(clilines)), 
               desc="reading client...", 
               ascii=False, ncols=75):
    if "pmdb_client_request_new@273" in clilines[i]:
        clilog.addcreate(clilines[i])
    if "raft_client_request_send_queue_add_locked@917" in clilines[i]:
        clilog.addsendtolead(clilines[i])
    if "CLI-REPL" in clilines[i]:
        clilog.addcompletedop(clilines[i])
    if "raft_client_update_leader_from_redirect@1219"in clilines[i]:
        clilog.addleadredirect(clilines[i])
file.close
cluster = []
for x in range(len(sf)):
        file = open(sf[x], "rb")
        lines= [l.decode('utf8', 'ignore') for l in file.readlines()]
        uuidkw="system_info_auto_detect_uuid"
        uuid=""
        for i in tqdm (range(len(lines)), 
                desc="reading server...", 
                ascii=False, ncols=75):
                line=lines[i]
                if uuidkw in line:
                        ar = line.split('=')
                        uuid = ar[1][:len(uuid)-1]
                        cluster.append(server(uuid))
                if "pmdb_sm_handler@1015" in line:
                    cluster[x].addleaderrecieve(line)
                if "raft_server_become_candidate@1962" in line:
                    cluster[x].addcandidate(line)
                if "raft_server_write_coalesced_entries@2556" in line:
                    cluster[x].addtimeouts(line)
                if "raft_server_send_msg@1686> AE_REQ" in line and "hb=0" in line and "sz=0" not in line:
                    cluster[x].addsendtofol(line)
                if "raft_server_peer_recv_handler@3800> AE_REQ" in line and "hb=0" in line and "sz=0" not in line:
                    cluster[x].addfolrecreq(line)
                if "raft_server_sync_thread" in line and "raft_server_has_unsynced_entries(): 1" in line:
                    cluster[x].addstartsync(line)
                if "raft_server_backend_sync@1253" in line:
                    cluster[x].addendsync(line)
                if "raft_server_try_update_follower_sync_idx@3579" in line:
                    cluster[x].addleadackfol(line)
                if "CLI-REPL" in line:
                    cluster[x].addreptocli(line)
                if "pmdb_sm_handler_client_rw_op@810" in line:
                    cluster[x].addlookup(line)
                if "pmdb_sm_handler_client_read@770" in line:
                    cluster[x].addread(line)
                if "raft_server_client_recv_handler@4324" in line:
                    cluster[x].addreadcomplete(line)
        file.close
reqnum=1
reqarr=[]
for i in tqdm (range(len(clilog.creates)), 
               desc="Client Creates...", 
               ascii=False, ncols=75):
        req=request(len(cluster))
        create=clilog.creates[i]
        optype = re.search('op=(.+?) tag', create)
        if optype:
                req.settype(optype.group(1))
        if req.gettype() =="write":
                numgrab = re.search(':0:0:0:(.+?) op=', create)
                if numgrab:
                        req.setnum(numgrab.group(1))
        else:
                hexgrab = re.search('ffffffffffffffff:ffffffffffffffff:ffffffffffffffff:(.+?) op=', create)
                if hexgrab:
                        hexnum=hexgrab.group(1)
                        num=int(hexnum,16)
                        req.setnum(num)
        createreqtime = grabtime(create)
        req.setcreatereqtime(createreqtime)
        reqarr.append(req)
for i in tqdm (range(len(reqarr)), 
               desc="Client send to Leader...", 
               ascii=False, ncols=75):
        if reqarr[i].gettype() =="write":
                opnum=reqarr[i].getnum()
        else:
                opnum=f'{reqarr[i].getnum():x}'
        for j in range(len(clilog.sendtolead)):
                if "."+str(opnum)+" msgid=" in clilog.sendtolead[j]:
                        clireqsentime =grabtime(clilog.sendtolead[j])
                        reqarr[i].setclientreqsendtime(clireqsentime)
                        idgrab = re.search('msgid=(.+?) nr', clilog.sendtolead[j])
                        if idgrab:
                                reqarr[i].setid(idgrab.group(1))
                        clilog.sendtolead.pop(j)
                        break
for i in tqdm (range(len(reqarr)), 
               desc="Client Recieve Leader...", 
               ascii=False, ncols=75):
        for j in range(len(clilog.opcomplete)):
                if "err=0:0" in clilog.opcomplete[j] and reqarr[i].getid() in clilog.opcomplete[j]:
                        clirectime =grabtime(clilog.opcomplete[j])
                        reqarr[i].setclientreqrectime(clirectime)
                        uuidgrab = re.search('CLI-REPL  (.+?) id=',clilog.opcomplete[j])
                        if uuidgrab:
                                leaduuid=uuidgrab.group(1)
                        clilog.opcomplete.pop(j)
                        break 
j=0
while j < len(clilog.opcomplete):
        if "err=0:0" not in clilog.opcomplete[j]:
                clilog.opcomplete.pop(j)
                continue
        j+=1
pbar = tqdm(desc="mult attempt finishes...")
collected=0
while 0 != len(clilog.opcomplete):
        for i in range(len(reqarr)):
                for j in range(len(clilog.opcomplete)):
                        if "err=0:0" in clilog.opcomplete[j] and reqarr[i].getid() in clilog.opcomplete[j]:
                                clirectime =grabtime(clilog.opcomplete[j])
                                reqarr[i].setclientreqrectime(clirectime)
                                reqarr[i].tookmulttries()
                                uuidgrab = re.search('CLI-REPL  (.+?) id=',clilog.opcomplete[j])
                                if uuidgrab:
                                        leaduuid=uuidgrab.group(1)
                                clilog.opcomplete.pop(j)
                                collected+=1
                                pbar.update(collected)
                                break 
pbar.close()
optracker=[]
for ser in cluster:
        if ser.uuid == leaduuid:
                for i in tqdm (range(len(reqarr)), 
                               desc="Leader Recieve Req...", 
                               ascii=False, ncols=75):
                        for j in range(len(ser.leaderrecieve)):
                                if reqarr[i].getid() in ser.leaderrecieve[j]:
                                        leadreqrecttime =grabtime(ser.leaderrecieve[j])
                                        reqarr[i].setleaderreqrectime(leadreqrecttime)
                                        ser.leaderrecieve.pop(j)
                                        break
                optracker.clear()
                for i in tqdm (range(len(reqarr)), 
                               desc="timeouts...", 
                               ascii=False, ncols=75):
                        for j in range(len(ser.timeouts)):
                                timeouttime =grabtime(ser.timeouts[j])
                                if float(reqarr[i].leaderreqrec) < float(timeouttime):
                                        entriegrab=ser.timeouts[j].split('Write coalesced entries: ')
                                        entries=int(entriegrab[1])
                                        reqarr[i].settimeouttime(timeouttime)
                                        reqarr[i].setrelatedentries(entries)
                                        if len(optracker)==0:
                                                if entries != 1:
                                                        optracker.append(tracker(ser.timeouts[j]))
                                                else:
                                                        ser.timeouts.pop(j)
                                        else:
                                                within = False
                                                for x in range(len(optracker)):
                                                        if optracker[x].comp_line(ser.timeouts[j]):
                                                                within=True
                                                                optracker[x].inc_uses()
                                                                if optracker[x].get_uses()==int(entries):
                                                                        ser.timeouts.pop(j)
                                                                        optracker.pop(x)
                                                                        break
                                                if not within:
                                                        if entries != 1:
                                                                optracker.append(tracker(ser.timeouts[j]))
                                                        else:
                                                                ser.timeouts.pop(j)
                                        break
                optracker.clear()
                for i in tqdm (range(len(reqarr)), 
                               desc="send to followers...", 
                               ascii=False, ncols=75):
                        for j in range(len(ser.sendtofol)):
                                sendtime =grabtime(ser.sendtofol[j])
                                if float(reqarr[i].timeout) < float(sendtime):
                                        crcgrab = re.search('crc=(.+?) ', ser.sendtofol[j])
                                        if crcgrab:
                                                reqarr[i].setcrc(crcgrab.group(1))
                                        reqarr[i].setsendfollowerstime(sendtime)
                                        nengrab = re.search('nen=(.+?) ', ser.sendtofol[j])
                                        if nengrab:
                                                nen=nengrab.group(1)
                                        if len(optracker)==0:
                                                if nen != 1:
                                                        optracker.append(tracker(ser.sendtofol[j]))
                                                else:
                                                        ser.sendtofol=remove_item(ser.sendtofol, reqarr[i].getcrc())
                                        else:
                                                within = False
                                                for x in range(len(optracker)):
                                                        if optracker[x].comp_line(ser.sendtofol[j]):
                                                                within=True
                                                                optracker[x].inc_uses()
                                                                if optracker[x].get_uses()==int(nen):
                                                                        ser.sendtofol=remove_item(ser.sendtofol, reqarr[i].getcrc())
                                                                        optracker.pop(x)
                                                                        break
                                                if not within:
                                                        if nen != 1:
                                                                optracker.append(tracker(ser.sendtofol[j]))
                                                        else:
                                                                ser.sendtofol=remove_item(ser.sendtofol, reqarr[i].getcrc())
                                        break
                for i in tqdm (range(len(reqarr)), 
                               desc="lookup...", 
                               ascii=False, ncols=75):
                        if reqarr[i].gettype()=="read":
                                for j in range(len(ser.lookup)):
                                        lookuptime=grabtime(ser.lookup[j])
                                        if float(lookuptime)>float(reqarr[i].leaderreqrec) and reqarr[i].getid() in ser.lookup[j]:
                                                reqarr[i].setlookup(lookuptime)
                                                ser.lookup.pop(j)
                                                break
                for i in tqdm (range(len(reqarr)), 
                               desc="read...", 
                               ascii=False, ncols=75):
                        for j in range(len(ser.read)):
                                if reqarr[i].getid() in ser.read[j]:
                                        readtime=grabtime(ser.read[j])
                                        reqarr[i].setread(readtime)
                                        ser.read.pop(j)
                                        break
                for i in tqdm (range(len(reqarr)), 
                               desc="read complete...", 
                               ascii=False, ncols=75):
                        for j in range(len(ser.readcomplete)):
                                if reqarr[i].getid() in ser.readcomplete[j]:
                                        readcompletetime=grabtime(ser.readcomplete[j])
                                        reqarr[i].setreadcomplete(readcompletetime)
                                        ser.readcomplete.pop(j)
                                        break

for sernum in range(len(cluster)):
        optracker.clear()
        if cluster[sernum].uuid != leaduuid:
                for i in tqdm (range(len(reqarr)), 
                               desc="server "+str(sernum+1)+" receive request...", 
                               ascii=False, ncols=75):
                        for j in range(len(cluster[sernum].folrecreq)):
                                if reqarr[i].getcrc() in cluster[sernum].folrecreq[j]:
                                        rectime =grabtime(cluster[sernum].folrecreq[j])
                                        reqarr[i].setfollowerreqrectime(rectime,cluster,cluster[sernum].uuid)
                                        nengrab = re.search('nen=(.+?) ', cluster[sernum].folrecreq[j])
                                        if nengrab:
                                                nen=nengrab.group(1)
                                        if len(optracker)==0:
                                                if nen != 1:
                                                        optracker.append(tracker(cluster[sernum].folrecreq[j]))
                                                else:
                                                        cluster[sernum].folrecreq=remove_item(cluster[sernum].folrecreq, reqarr[i].getcrc())
                                        else:
                                                within = False
                                                for x in range(len(optracker)):
                                                        if optracker[x].comp_line(cluster[sernum].folrecreq[j]):
                                                                within=True
                                                                optracker[x].inc_uses()
                                                                if optracker[x].get_uses()==int(nen):
                                                                        cluster[sernum].folrecreq=remove_item(cluster[sernum].folrecreq, reqarr[i].getcrc())
                                                                        optracker.pop(x)
                                                                        break
                                                if not within:
                                                        if nen != 1:
                                                                optracker.append(tracker(cluster[sernum].folrecreq[j]))
                                                        else:
                                                                cluster[sernum].folrecreq=remove_item(cluster[sernum].folrecreq, reqarr[i].getcrc())
                                        break
        optracker.clear()
        for i in tqdm (range(len(reqarr)), 
                       desc="server "+str(sernum+1)+" sync start...", 
                       ascii=False, ncols=75):
                if reqarr[i].gettype()=="write":
                        if cluster[sernum].uuid == leaduuid:
                                comptime=reqarr[i].timeout
                                if i==len(reqarr)-1:
                                        nextcomptime=reqarr[i].timeout
                                else:
                                        nextcomptime=reqarr[i+1].timeout
                        else:
                                comptime=reqarr[i].followerreqrec[sernum]
                                if i==len(reqarr)-1:
                                        nextcomptime=reqarr[i].followerreqrec[sernum]
                                else:
                                        nextcomptime=reqarr[i+1].followerreqrec[sernum]
                        for j in range(len(cluster[sernum].startsync)):
                                syncstarttime=grabtime(cluster[sernum].startsync[j])
                                if float(syncstarttime) > float(comptime):
                                        sugrab = re.search('ei(.+?) ', cluster[sernum].startsync[j])
                                        if sugrab:
                                                su=sugrab.group(1)
                                                splitsu=su.split(':')
                                                u=splitsu[2]
                                        reqarr[i].setsynctime(syncstarttime,u,cluster,cluster[sernum].uuid)
                                        if len(optracker)==0:
                                                if reqarr[i].getrelatedentries() != 1:
                                                        optracker.append(tracker(cluster[sernum].startsync[j]))
                                                else:
                                                        if float(syncstarttime) < float(nextcomptime):
                                                                cluster[sernum].startsync.pop(j)
                                        else:
                                                within = False
                                                for x in range(len(optracker)):
                                                        if optracker[x].comp_line(cluster[sernum].startsync[j]):
                                                                within=True
                                                                optracker[x].inc_uses()
                                                                if optracker[x].get_uses()==int(reqarr[i].getrelatedentries()):
                                                                        if float(syncstarttime) < float(nextcomptime):
                                                                                cluster[sernum].startsync.pop(j)
                                                                                optracker.pop(x)
                                                                break
                                                if not within:
                                                        if reqarr[i].getrelatedentries() != 1:
                                                                optracker.append(tracker(cluster[sernum].startsync[j]))
                                                        else:
                                                                if float(syncstarttime) < float(nextcomptime):
                                                                        cluster[sernum].startsync.pop(j)
                                        break
        optracker.clear()
        for i in tqdm (range(len(reqarr)), 
                       desc="server "+str(sernum+1)+" sync end...", 
                       ascii=False, ncols=75):
                if reqarr[i].gettype()=="write":
                        if i==len(reqarr)-1:
                                nexti=i
                        else:
                                nexti=i+1
                        for j in range(len(cluster[sernum].endsync)):
                                endsync=grabtime(cluster[sernum].endsync[j])
                                if float(endsync) > float(reqarr[i].followersync[sernum]):
                                        sugrab = re.search('ei(.+?) ', cluster[sernum].endsync[j])
                                        if sugrab:
                                                su=sugrab.group(1)
                                                splitsu=su.split(':')
                                                u=splitsu[2]
                                        reqarr[i].setsyncendtime(endsync,cluster,cluster[sernum].uuid)
                                        if len(optracker)==0:
                                                if reqarr[i].getrelatedentries() != 1:
                                                        optracker.append(tracker(cluster[sernum].endsync[j]))
                                                else:
                                                        if float(endsync) < float(reqarr[nexti].followersync[sernum]):
                                                                cluster[sernum].endsync.pop(j)
                                        else:
                                                within = False
                                                for x in range(len(optracker)):
                                                        if optracker[x].comp_line(cluster[sernum].endsync[j]):
                                                                within=True
                                                                optracker[x].inc_uses()
                                                                if optracker[x].get_uses()==int(reqarr[i].getrelatedentries()):
                                                                        if float(endsync) < float(reqarr[nexti].followersync[sernum]):
                                                                                cluster[sernum].endsync.pop(j)
                                                                                optracker.pop(x)
                                                                        break
                                                if not within:
                                                        if reqarr[i].getrelatedentries() != 1:
                                                                optracker.append(tracker(cluster[sernum].endsync[j]))
                                                        else:
                                                                if float(endsync) < float(reqarr[nexti].followersync[sernum]):
                                                                        cluster[sernum].endsync.pop(j)
                                        break
        for i in tqdm (range(len(reqarr)), 
                       desc="server "+str(sernum+1)+" status...", 
                       ascii=False, ncols=75):
                for j in range(len(cluster[sernum].candidate)):
                        candtime=grabtime(cluster[sernum].candidate[j])
                        if i==0:
                                starttime=reqarr[i].createreq
                        else:
                                starttime=reqarr[i-1].clientreqrec
                        if float(candtime) > float(starttime) and float(candtime) < float(reqarr[i].clientreqrec):
                                reqarr[i].setserverstatus(cluster,cluster[sernum].uuid,candtime)

for sernum in range(len(cluster)):
        if cluster[sernum].uuid == leaduuid:
                optracker.clear()
                for i in tqdm (range(len(reqarr)), 
                               desc="leader ack...", 
                               ascii=False, ncols=75):
                        for ackedserver in range(len(cluster)):
                                if cluster[ackedserver].uuid != leaduuid:
                                        for j in range(len(cluster[sernum].leadackfol)):
                                                        ackstime =grabtime(cluster[sernum].leadackfol[j])
                                                        uuidgrab = re.search('err=0:0 (.+?) new-sync', cluster[sernum].leadackfol[j])
                                                        if uuidgrab:
                                                                uuid=uuidgrab.group(1)
                                                        idxgrab = cluster[sernum].leadackfol[j].split("new-sync-idx=")
                                                        idx=idxgrab[1]
                                                        if float(ackstime) > float(reqarr[i].followersyncend[ackedserver]) and int(idx)>=int(reqarr[i].uofsync[ackedserver]) and cluster[ackedserver].uuid == uuid:
                                                                reqarr[i].setackstime(ackstime,cluster,uuid)
                                                                temp=i+1
                                                                used=False
                                                                while temp < len(reqarr):
                                                                        if float(ackstime)>float(reqarr[temp].followersyncend[ackedserver]):
                                                                                used=True
                                                                                break
                                                                        temp+=1
                                                                if not used:
                                                                        cluster[sernum].leadackfol.pop(j)
                                                                break
                optracker.clear()
                for i in tqdm (range(len(reqarr)), 
                               desc="Leader to Client...", 
                               ascii=False, ncols=75):
                        for j in range(len(cluster[sernum].reptocli)):
                            if "err=0:0" in cluster[sernum].reptocli[j] and reqarr[i].getid() in cluster[sernum].reptocli[j]:
                                repltime =grabtime(cluster[sernum].reptocli[j])
                                reqarr[i].setreptoclitime(repltime)
                                cluster[sernum].reptocli.pop(j)
                                break
are_writes=False
are_reads=False
for req in reqarr:
        optype=req.gettype()
        if optype=="write":
                are_writes=True
        if optype=="read":
                are_reads=True
f = open(args.output, 'w')
writecsv = csv.writer(f)
if are_writes:
        writecsv.writerow(makewritelabels(cluster))
        for i in tqdm (range(len(reqarr)), 
                        desc="printing write info...", 
                        ascii=False, ncols=75):
                if reqarr[i].gettype()=="write":
                        writecsv.writerow(reqarr[i].getdata())
if are_reads:
        writecsv.writerow(makereadlabels(cluster))
        for i in tqdm (range(len(reqarr)), 
                        desc="printing read info...", 
                        ascii=False, ncols=75):
                if reqarr[i].gettype()=="read":
                        writecsv.writerow(reqarr[i].getdata())
f.close()