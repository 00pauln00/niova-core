package main // niova control interface

import (
	"encoding/json"
	//	"fmt"
	"github.com/google/uuid"
	"io/ioutil"
	"log"
	//	"math/rand"
	"os"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	maxPendingCmdsEP  = 32
	maxOutFileSize    = 4*1024 ^ 2
	outFileTimeoutSec = 2
	outFilePollMsec   = 1
	EPtimeoutSec      = 10.0
)

type Time struct {
	WrappedTime time.Time `json:"time"`
}

type SystemInfo struct {
	CurrentTime             Time      `json:"current_time"`
	StartTime               Time      `json:"start_time"`
	Pid                     int       `json:"pid"`
	UUID                    uuid.UUID `json:"uuid"`
	CtlInterfacePath        string    `json:"ctl_interface_path"`
	CommandLine             string    `json:"command_line"`
	UtsNodename             string    `json:"uts.nodename"`
	UtsSysname              string    `json:"uts.sysname"`
	UtsRelease              string    `json:"uts.release"`
	UtsVersion              string    `json:"uts.version"`
	UtsMachine              string    `json:"uts.machine"`
	RusageUserCPUTimeUsed   float64   `json:"rusage.user_cpu_time_used"`
	RusageSystemCPUTimeUsed float64   `json:"rusage.system_cpu_time_used"`
	RusageMaxRss            int       `json:"rusage.max_rss"`
	RusageMinFault          int       `json:"rusage.min_fault"`
	RusageMajFault          int       `json:"rusage.maj_fault"`
	RusageInBlock           int       `json:"rusage.in_block"`
	RusageOutBlock          int       `json:"rusage.out_block"`
	RusageVolCtsw           int       `json:"rusage.vol_ctsw"`
	RusageInvolCtsw         int       `json:"rusage.invol_ctsw"`
}

type CtlIfOut struct {
	SysInfo SystemInfo `json:"system_info"`
}

type NcsiEP struct {
	Uuid         uuid.UUID `json:"-"` // Field is ignored by json
	Path         string    `json:"-"`
	Name         string    `json:"name"`
	NiovaSvcType string    `json:"type"`
	Port         int       `json:"port"`
	LastReport   time.Time `json:"-"`
	Alive        bool      `json:"responsive"`
	EPInfo       CtlIfOut  `json:"ep_info"`
	pending      uint64
}

type epCommand struct {
	ep      *NcsiEP
	cmd     string
	fn      string
	outJSON []byte
	err     error
}

func chompQuotes(data []byte) []byte {
	s := string(data)

	// Check for quotes
	if len(s) > 0 {
		if s[0] == '"' {
			s = s[1:]
		}
		if s[len(s)-1] == '"' {
			s = s[:len(s)-1]
		}
	}

	return []byte(s)
}

// custom UnmarshalJSON method used for handling various timestamp formats.
func (t *Time) UnmarshalJSON(data []byte) error {
	var err error

	data = chompQuotes(data)

	if err = json.Unmarshal(data, t.WrappedTime); err == nil {
		return nil
	}
	const layout = "Mon Jan 02 15:04:05 MST 2006"

	t.WrappedTime, err = time.Parse(layout, string(data))

	return err
}

func (cmd *epCommand) getOutFnam() string {
	return cmd.ep.EProot() + "/output/" + cmd.fn
}

func (cmd *epCommand) getInFnam() string {
	return cmd.ep.EProot() + "/input/" + cmd.fn
}

func (cmd *epCommand) getCmdBuf() []byte {
	return []byte(cmd.cmd)
}

func (cmd *epCommand) getOutJSON() []byte {
	return []byte(cmd.outJSON)
}

func (cmd *epCommand) pollOutFile() error {
	for i := outFileTimeoutSec * 1000; i > 0; i-- {

		var tmp_stb syscall.Stat_t
		err := syscall.Stat(cmd.getOutFnam(), &tmp_stb)

		if err == syscall.ENOENT {
			time.Sleep(outFilePollMsec * time.Millisecond)
			continue

		} else if err != nil {
			log.Printf("syscall.Stat('%s'): %s\n",
				cmd.getOutFnam(), err)
			return err

		} else if tmp_stb.Size > maxOutFileSize {
			return syscall.E2BIG
		}
		// Success
		return nil
	}

	return syscall.ETIMEDOUT
}

func (cmd *epCommand) complete() {
	if cmd.err = cmd.pollOutFile(); cmd.err != nil {
		return
	}

	// Try to read the file
	cmd.outJSON, cmd.err = ioutil.ReadFile(cmd.getOutFnam())

	return
}

func (cmd *epCommand) prep() {
	cmd.fn = "ncsiep_" + strconv.FormatInt(int64(os.Getpid()), 10) +
		"_" + strconv.FormatInt(int64(time.Now().Nanosecond()), 10)

	cmd.cmd = cmd.cmd + "\nOUTFILE /" + cmd.fn + "\n"
}

func (cmd *epCommand) write() {
	cmd.err = ioutil.WriteFile(cmd.getInFnam(), cmd.getCmdBuf(), 0644)
	if cmd.err != nil {
		log.Printf("ioutil.WriteFile(): %s", cmd.err)
		return
	}
}

func (cmd *epCommand) issue() {
	if cmd.err = cmd.ep.incPending(); cmd.err != nil {
		return
	}
	cmd.prep()

	cmd.write()

	if cmd.err == nil {
		cmd.complete()
	}

	cmd.ep.decPending()
}

func (ep *NcsiEP) incPending() error {
	if atomic.LoadUint64(&ep.pending) > maxPendingCmdsEP {
		return syscall.EAGAIN
	}

	var tmp uint64 = 1
	atomic.AddUint64(&ep.pending, tmp)
	return nil
}

func (ep *NcsiEP) decPending() {
	var tmp uint64 = 1
	atomic.AddUint64(&ep.pending, -tmp)

	if atomic.LoadUint64(&ep.pending) < 0 {
		panic("ep.pending is negative")
	}
}

func (ep *NcsiEP) EProot() string {
	return ep.Path
}

func (ep *NcsiEP) getSysinfo() error {
	cmd := epCommand{ep, "GET /system_info/.*", "", nil, nil}
	cmd.issue()
	if cmd.err != nil {
		return cmd.err
	}

	var err error
	var ctlifout CtlIfOut
	if err = json.Unmarshal(cmd.getOutJSON(), &ctlifout); err != nil {
		if ute, ok := err.(*json.UnmarshalTypeError); ok {
			log.Printf("UnmarshalTypeError %v - %v - %v\n",
				ute.Value, ute.Type, ute.Offset)
		} else {
			log.Printf("Other error: %s\n", err)
			log.Printf("Contents: %s\n", string(cmd.getOutJSON()))
		}

	} else {
		ep.EPInfo = ctlifout
		ep.LastReport = ep.EPInfo.SysInfo.CurrentTime.WrappedTime
	}

	//	log.Printf("%+v \n", ep)

	return err
}

func (ep *NcsiEP) Detect() error {
	err := ep.getSysinfo()

	if time.Since(ep.LastReport) > time.Second*EPtimeoutSec {
		ep.Alive = false
	} else if !ep.Alive {
		ep.Alive = true
	}

	return err
}

func (ep *NcsiEP) Check() error {
	return nil
}
