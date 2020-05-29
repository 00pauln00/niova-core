package main // niova control interface

import (
	//	"encoding/json"
	"github.com/google/uuid"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"strconv"
	"syscall"
	"time"
)

type NcsiEP struct {
	Uuid         uuid.UUID `json:"-"` // Field is ignored by json
	Path         string    `json:"-"`
	Name         string    `json:"name"`
	NiovaSvcType string    `json:"type"`
	Port         int       `json:"port"`
	LastReport   time.Time `json:"-"`
	Alive        bool      `json:"responsive"`
}

func loadOutfile(outf string) ([]byte, error) {
	var i int = 0

	for i < 100 {
		var tmp_stb syscall.Stat_t
		err := syscall.Stat(outf, &tmp_stb)
		if err == syscall.ENOENT {
			time.Sleep(1 * time.Millisecond)

		} else if err != nil {
			return nil, err
		}

		return ioutil.ReadFile(outf)
	}
	return nil, syscall.ETIMEDOUT
}

func (ep *NcsiEP) issueCmd(cmd string) error {
	r := rand.New(rand.NewSource(int64(os.Getpid())))
	var fn string = "ncsiep_" + strconv.FormatInt(int64(os.Getpid()), 10) +
		"_" + strconv.FormatInt(int64(r.Uint32()), 10)
	var out string = "\nOUTFILE /" + fn + "\n"

	err := ioutil.WriteFile(ep.Path+"/input/"+fn, []byte(cmd+out),
		0644)
	if err != nil {
		log.Printf("ioutil.WriteFile(): %s", err)
		return err
	}

	contents, err := loadOutfile(ep.Path + "/output/" + fn)

	log.Print(contents)

	return err
}

func (ep *NcsiEP) getAll() error {
	return ep.issueCmd("GET /.*/.*/.*/.*\n")
}

func (ep *NcsiEP) Detect() error {
	return ep.getAll()
}

func (ep *NcsiEP) Check() error {
	return nil
}
