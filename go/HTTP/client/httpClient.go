package httpClient

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"errors"
	log "github.com/sirupsen/logrus"
)

func service_Request(request *http.Request) ([]byte, error) {

	request.Header.Set("Content-Type", "application/json; charset=utf-8")
	httpClient := &http.Client{}

	response, err := httpClient.Do(request)
	if err != nil {
		log.Error("(HTTP CLIENT DO)",err)
		return nil, err
	}

	switch response.StatusCode {
	case 200:
		//Serviced
		defer response.Body.Close()
		return ioutil.ReadAll(response.Body)
	case 503:
		//Service not found, returned for timeout
		return nil,errors.New("Server timed out")
	}
	return nil,nil
}

func HTTP_Request(requestBody []byte, address string, put bool) ([]byte, error) {
	var request *http.Request
	var err error

	connectionAddress := "http://" + address
	if put {
		request, err = http.NewRequest(http.MethodPut, connectionAddress, bytes.NewBuffer(requestBody))
	} else {
		request, err = http.NewRequest(http.MethodGet, connectionAddress, bytes.NewBuffer(requestBody))
	}
	if err != nil {
		log.Error(err)
		return nil, err
	}
	return service_Request(request)
}
