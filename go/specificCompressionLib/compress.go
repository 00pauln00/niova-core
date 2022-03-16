package specificCompressionLib

import (
	"encoding/hex"
	"strings"
	"strconv"
	"errors"
	"bytes"
	"reflect"
)

type UUID string
type IPV4 string
type Num_1 string
type Num_2 string


func CompressStructure(StructData interface{}) (string, error) {
        structExtract := reflect.ValueOf(StructData)
        var compressedString string

        for i := 0; i < structExtract.NumField(); i++ {
                class := structExtract.Field(i).Type()
                value := structExtract.Field(i).String()
                dataType, size := sizeOfType(class.String())

                var compressedEntity string
                var err error
                switch dataType {
                case "UUID":
                        compressedEntity, err = CompressUUID(value)

                case "IPV4":
                        compressedEntity, err = CompressIPV4(value)

                case "Num":
                        compressedEntity, err = CompressStringNumber(value, size)
                }

                if err != nil {
                        return compressedString, err
                }
                compressedString += compressedEntity
        }
        return compressedString, nil
}

func sizeOfType(compossedDataType string) (string, int) {
        dataTypeWSize := strings.Split(compossedDataType, ".")[1]
        dataTypeSlice := strings.Split(dataTypeWSize, "_")
        dataType := dataTypeSlice[0]
        var size int
        switch dataType {
        case "UUID":
                size = 16
        case "IPV4":
                size = 4
        case "Num":
                size, _ = strconv.Atoi(dataTypeSlice[1])
        }
        return dataType, size
}

func extractBytes(data string, offset *int, size int) []byte {
        var addon, addonPrev int
        var returnBytes []byte
        checkByte195 := []byte{byte(195)}
        checkByte194 := []byte{byte(194)}
        for {
                returnBytes = []byte(data[*offset : *offset+size])
                //Check if val has 194 or 195
                addon = bytes.Count(returnBytes, checkByte195) + bytes.Count(returnBytes, checkByte194)
                if addon == addonPrev {
                        break
                }
                size += addon
                addonPrev = addon
        }
        *offset = *offset + size
        return returnBytes
}

func DecompressStructure(StructData interface{}, compressedData string) {
        structExtract := reflect.ValueOf(StructData).Elem()
        offset := 0
        for i := 0; i < structExtract.NumField(); i++ {
                class := structExtract.Field(i).Type()
                dataType, size := sizeOfType(class.String())
                fieldValueBytes := extractBytes(compressedData, &offset, size)

                //Decompress data
                var stringData string
                switch dataType {
                case "UUID":
                        stringData, _ = DecompressUUID(string(fieldValueBytes))
                case "IPV4":
                        stringData = DecompressIPV4(string(fieldValueBytes))
                case "Num":
                        stringData = DecompressNumber(string(fieldValueBytes))
                }

                //Fill the struct
                value := reflect.ValueOf(stringData).Convert(class)
                field := structExtract.FieldByName(structExtract.Type().Field(i).Name)
                if field.CanSet() {
                        field.Set(value)
                }
        }
}

func CompressUUID(uuid string) (string,error) {
	replaced := strings.Replace(uuid,"-","",4)
	byteArray, err := hex.DecodeString(replaced)
        if err!= nil{
                return "",err
        }

	return string(byteArray),nil
}

func DecompressUUID(cUUID string) (string,error) {
	if len(cUUID) < 16 {
		return "", errors.New("Failed to parse compressed UUID")
	}
	uByteArray := []byte(cUUID)
        uhex := hex.EncodeToString(uByteArray)
	//Put "-" seprator
	uuid := uhex[:8]+"-"+uhex[8:12]+"-"+uhex[12:16]+"-"+uhex[16:20]+"-"+uhex[20:]
	return uuid, nil
}

func CompressIPV4(ip string) (string,error) {
	replaced := strings.Split(ip,".")
	var byteArray []byte
	for _,octet := range replaced{
		inte8,err := strconv.Atoi(octet)
		if err != nil {
			return "",err
		}
		byteArray = append(byteArray,uint8(inte8))
	}
	return string(byteArray),nil
}

func DecompressIPV4(cIPV4 string) string {
	uByteArray := []byte(cIPV4)
	var ipAddr string
	for _, octet := range uByteArray{
		ipAddr += strconv.Itoa(int(octet))
		ipAddr += "."
	}
	return ipAddr[:len(ipAddr)-1]
}

func CompressStringNumber(snumber string,nobytes int) (string,error){
	number,err := strconv.Atoi(snumber)
        if err != nil{
                return "",err
        }
	return CompressNumber(number,nobytes)
}

func CompressNumber(number int,nobytes int) (string,error){
	//Convert to binary sequence
	binseq := strconv.FormatInt(int64(number),2)
	if (len(binseq)%8 != 0) {
		binseq = strings.Repeat("0",8-len(binseq)%8) + binseq
	}
	var outstring string
	for i := 0; i < len(binseq)/8; i++{
		start := i*8
		end := (i+1)*8
		intrep,err := strconv.ParseInt(binseq[start:end],2,64)
		if err != nil {
			return "",nil
		}
		outstring += string(intrep)
	}
	if (len(outstring) < nobytes) {
		outstring = strings.Repeat(string(0),nobytes-len(outstring))+ outstring
	}
	return outstring,nil
}

func DecompressNumber(cnumber string) string{
	var binseq string
	flag := true
	for _,char := range cnumber{
		if ((flag == true) && (string(char) == string(0))){
			continue
		}
		flag = false
		seq := strconv.FormatInt(int64(byte(char)),2)
		if (len(seq)%8 != 0){
			seq = strings.Repeat("0",8-len(seq)%8) + seq
		}
		binseq += seq
	}
	number,_ := strconv.ParseInt(binseq,2,64)
	return strconv.Itoa(int(number))
}
