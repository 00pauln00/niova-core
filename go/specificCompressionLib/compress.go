package specificCompressionLib

import (
	"bytes"
	"encoding/hex"
	"errors"
	"reflect"
	"strconv"
	"strings"
)

type StringUUID string
type StringIPV4 string

func CompressStructure(StructData interface{}) (string, error) {
	structExtract := reflect.ValueOf(StructData)
	var compressedString string

	for i := 0; i < structExtract.NumField(); i++ {
		class := structExtract.Field(i).Type()
		value := structExtract.Field(i)
		dataType, size := sizeOfType(class.String())

		var compressedEntity string
		var err error
		switch dataType {
		case "StringUUID":
			compressedEntity, err = CompressUUID(value.String())

		case "StringIPV4":
			compressedEntity, err = CompressIPV4(value.String())

		case "uint8":
			if size > 1 {
				byteArray := value.Interface().([16]uint8)
				compressedEntity = string(byteArray[:])
				break
			}
			compressedEntity = string(value.Uint())

		case "uint16":
			compressedEntity, err = CompressInteger(int(value.Uint()), size)
		}

		if err != nil {
			return compressedString, err
		}
		compressedString += compressedEntity
	}
	return compressedString, nil
}


func sizeOfType(composedDataType string) (string, int) {
	dataTypeWLib := strings.Split(composedDataType, ".")
	var dataType, dataTypeCount string
	if len(dataTypeWLib) > 1 {
		dataTypeSlice := strings.Split(dataTypeWLib[1], "_")
		dataType = dataTypeSlice[0]
		if len(dataTypeSlice) > 1 {
			dataTypeCount = dataTypeSlice[1]
		}
	} else {
		dataTypeSlice := strings.Split(dataTypeWLib[0], "]")
		if len(dataTypeSlice) > 1 {
			dataType = dataTypeSlice[1]
			dataTypeCount = dataTypeSlice[0][1:]
		} else {
			dataType = dataTypeSlice[0]
		}
	}

	var size int
	switch dataType {
	case "StringUUID":
		size = 16

	case "StringIPV4":
		size = 4

	case "uint8":
		size = 1
		multiplier, err := strconv.Atoi(dataTypeCount)
		if err == nil {
			size = size * multiplier
		}

	case "uint16":
		size = 2
	}

	return dataType, size
}


func extractBytes(data string, offset *int, size int) []byte {
	var returnBytes []byte
	returnBytes = []byte(data[*offset : *offset+size])
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
		var stringData interface{}
		switch dataType {
		case "UUID":
			stringData, _ = DecompressUUID(string(fieldValueBytes))
		case "IPV4":
			stringData = DecompressIPV4(string(fieldValueBytes))
		case "Num":
			stringData = DecompressInteger(string(fieldValueBytes))
		case "uint8":
			if size > 1 {
				var array [16]uint8
				copy(array[:], fieldValueBytes)
				stringData = array
			} else {
				stringData = fieldValueBytes[0]
			}
		case "uint16":
			val := DecompressInteger(string(fieldValueBytes))
			stringData, _ = strconv.Atoi(val)
		}

		//Fill the struct
		value := reflect.ValueOf(stringData).Convert(class)
		field := structExtract.FieldByName(structExtract.Type().Field(i).Name)
		if field.CanSet() {
			field.Set(value)
		}
	}
}

func CompressUUID(uuid string) (string, error) {
	replaced := strings.Replace(uuid, "-", "", 4)
	byteArray, err := hex.DecodeString(replaced)
	if err != nil {
		return "", err
	}

	return string(byteArray), nil
}

func DecompressUUID(cUUID string) (string, error) {
	if len(cUUID) < 16 {
		return "", errors.New("Failed to parse compressed UUID")
	}
	uByteArray := []byte(cUUID)
	uhex := hex.EncodeToString(uByteArray)
	//Put "-" seprator
	uuid := uhex[:8] + "-" + uhex[8:12] + "-" + uhex[12:16] + "-" + uhex[16:20] + "-" + uhex[20:]
	return uuid, nil
}

func CompressIPV4(ip string) (string, error) {
	replaced := strings.Split(ip, ".")
	var byteArray []byte
	for _, octet := range replaced {
		inte8, err := strconv.Atoi(octet)
		if err != nil {
			return "", err
		}
		byteArray = append(byteArray, uint8(inte8))
	}
	return string(byteArray), nil
}

func DecompressIPV4(cIPV4 string) string {
	uByteArray := []byte(cIPV4)
	var ipAddr string
	for _, octet := range uByteArray {
		ipAddr += strconv.Itoa(int(octet))
		ipAddr += "."
	}
	return ipAddr[:len(ipAddr)-1]
}

func CompressStringInteger(snumber string, nobytes int) (string, error) {
	number, err := strconv.Atoi(snumber)
	if err != nil {
		return "", err
	}
	return CompressInteger(number, nobytes)
}

func CompressInteger(number int, nobytes int) (string, error) {
	//Convert to binary sequence
	binseq := strconv.FormatInt(int64(number), 2)
	if len(binseq)%8 != 0 {
		binseq = strings.Repeat("0", 8-len(binseq)%8) + binseq
	}
	var outByte []byte
	for i := 0; i < len(binseq)/8; i++ {
		start := i * 8
		end := (i + 1) * 8
		intrep, err := strconv.ParseInt(binseq[start:end], 2, 64)
		if err != nil {
			return "", nil
		}
		outByte = append(outByte, byte(uint8(intrep)))
	}
	if len(outByte) < nobytes {
		byte0 := []byte{0}
		outByte = append(bytes.Repeat(byte0, nobytes-len(outByte)), outByte...)
	}

	return string(outByte), nil
}

func DecompressInteger(cnumber string) string {
	var binseq string
	flag := true
	for _, char := range []byte(cnumber) {
		if flag && (string(char) == string(0)) {
			continue
		}
		flag = false
		seq := strconv.FormatInt(int64(char), 2)
		if len(seq)%8 != 0 {
			seq = strings.Repeat("0", 8-len(seq)%8) + seq
		}
		binseq += seq
	}
	number, _ := strconv.ParseInt(binseq, 2, 64)
	return strconv.Itoa(int(number))
}
