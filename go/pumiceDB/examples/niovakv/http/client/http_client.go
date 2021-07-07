package httpclient

import (
	"bytes"
	"encoding/gob"
	"io/ioutil"
	"net/http"
	"niovakv/niovakvlib"

	log "github.com/sirupsen/logrus"
)

func WriteRequest(reqobj *niovakvlib.NiovaKV, addr, port string) (*niovakvlib.NiovaKVResponse, error) {
	var Data bytes.Buffer
	responseObj := niovakvlib.NiovaKVResponse{}

	enc := gob.NewEncoder(&Data)
	err := enc.Encode(reqobj)
	if err != nil {
		log.Error(err)
		return &responseObj, err
	}

	connString := "http://" + addr + ":" + port
	req, err := http.NewRequest(http.MethodPut, connString, bytes.NewBuffer(Data.Bytes()))
	if err != nil {
		log.Error(err)
		return &responseObj, err
	}

	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	client := &http.Client{}
	resp, err := client.Do(req)

	if err != nil {
		log.Error(err)
		return &responseObj, err
	}

	defer resp.Body.Close()
	bodyBytes, _ := ioutil.ReadAll(resp.Body)
	//Unmarshal the response.
	// Convert response body to reqobj struct
	dec := gob.NewDecoder(bytes.NewBuffer(bodyBytes))
	err = dec.Decode(&responseObj)
	if err == nil {
		log.Info("Response after operation:  ", reqobj.InputOps)
		log.Info("Status of write operation :", responseObj.RespStatus)
	}
	return &responseObj, err
}

func ReadRequest(reqobj *niovakvlib.NiovaKV, addr, port string) (*niovakvlib.NiovaKVResponse, error) {
	var request bytes.Buffer
	responseObj := niovakvlib.NiovaKVResponse{}

	enc := gob.NewEncoder(&request)
	err := enc.Encode(reqobj)
	if err != nil {
		log.Error(err)
		return &responseObj, err
	}

	connString := "http://" + addr + ":" + port
	req, err := http.NewRequest(http.MethodGet, connString, bytes.NewBuffer(request.Bytes()))
	if err != nil {
		log.Error(err)
		return &responseObj, err
	}

	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	client := &http.Client{}
	resp, err := client.Do(req)

	if err != nil {
		log.Error(err)
		return &responseObj, err
	}

	defer resp.Body.Close()
	bodyBytes, _ := ioutil.ReadAll(resp.Body)
	//Unmarshal the response.
	// Convert response body to reqobj struct
	dec := gob.NewDecoder(bytes.NewBuffer(bodyBytes))
	err = dec.Decode(&responseObj)
	if err == nil {
		log.Info("Response after operation:  ", reqobj.InputOps)
		log.Info("Value returned :", string(responseObj.RespValue))
	}
	return &responseObj, err
}
