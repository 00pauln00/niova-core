package httpclient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"niovakv/niovakvlib"

	log "github.com/sirupsen/logrus"
)

func SendRequest(reqobj *niovakvlib.NiovaKV, addr, port string) error {

	fmt.Println("Performing Http Request...")
	jsonReq, err := json.Marshal(reqobj)
	connString := "http://" + addr + ":" + port
	req, err := http.NewRequest(http.MethodPut, connString, bytes.NewBuffer(jsonReq))
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Error(err)
	}

	defer resp.Body.Close()
	bodyBytes, _ := ioutil.ReadAll(resp.Body)

	//Unmarshal the response.
	responeObj := niovakvlib.NiovaKVResponse{}
	// Convert response body to reqobj struct
	errUnmarshal := json.Unmarshal(bodyBytes, &responeObj)
	if errUnmarshal == nil {
		log.Info("Request object after successful operation:", responeObj)
	}
	return err
}
