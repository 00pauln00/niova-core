package httpclient

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"errors"
	log "github.com/sirupsen/logrus"
)

func serviceRequest(req *http.Request) ([]byte, error) {

	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	/*
		Check if response if ok,
		If not, fill the response obj with desired values
		If so, fill the resp obj with received values
	*/
	switch resp.StatusCode {
	case 200:
		//Serviced
		defer resp.Body.Close()
		return ioutil.ReadAll(resp.Body)
	case 503:
		//Service not found, returned for timeout
		return nil,errors.New("Server timed out")
	}
	return nil,nil
}

func Request(reqByteArray []byte, addr string, put bool) ([]byte, error) {
	var req *http.Request
	var err error

	connection_addr := "http://" + addr
	if put {
		req, err = http.NewRequest(http.MethodPut, connection_addr, bytes.NewBuffer(reqByteArray))
	} else {
		req, err = http.NewRequest(http.MethodGet, connection_addr, bytes.NewBuffer(reqByteArray))
	}
	if err != nil {
		log.Error(err)
		return nil, err
	}
	return serviceRequest(req)
}
