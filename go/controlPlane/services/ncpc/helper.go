package main

import (
	"encoding/json"
	"io/ioutil"
)

//Write to Json
func (cli *clientHandler) write2Json(toJson map[string][]opData) {
	file, _ := json.MarshalIndent(toJson, "", " ")
	_ = ioutil.WriteFile(cli.resultFile+".json", file, 0644)
}

func (cli *clientHandler) putNISDInfo() map[string]nisdData {
	data := cli.clientAPIObj.GetMembership()
	nisdDataMap := make(map[string]nisdData)
	for _, node := range data {
		if (node.Tags["Type"] == "LOOKOUT") && (node.Status == "alive") {
			for cuuid, value := range node.Tags {
				uuid, err := compressionLib.DecompressUUID(cuuid)
				if err != nil {
					CompressedStatus := value[1]
					CompressedWriteMeta := value[1:3]

					//Decompress
					thisNISDData := nisdData{}
					thisNISDData.UUID = uuid
					if string(CompressedStatus) == "1" {
						thisNISDData.Status = "Alive"
					} else {
						thisNISDData.Status = "Dead"
					}

					thisNISDData.WriteSize = compressionLib.DecompressNumber(CompressedWriteMeta)
					nisdDataMap[uuid] = thisNISDData
				}
			}
		}
	}
	return nisdDataMap
}
