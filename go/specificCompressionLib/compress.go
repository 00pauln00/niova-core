package specificCompressionLib

import (
	"encoding/hex"
	"encoding/binary"
	"errors"
	"reflect"
	"net"
	"strconv"
	"strings"
)


/*
Func name : CompressStructure
Description : This function compress the structure data to string. The size of the string 
matches the actual information entropy.

Allowed data types:
net.IP (Only IPV4) //4bytes
[16]uint8 //16bytes
uint8 //1byte
uint16 //2bytes
*/
func CompressStructure(StructData interface{}) (string, error) {
	//Representing in reflect format to get struct fields
	structExtract := reflect.ValueOf(StructData)
	var compressedOutput string

	//Iterating over fields and compress their values
	for i := 0; i < structExtract.NumField(); i++ {
		//Get data type and value of the field
		class := structExtract.Field(i).Type()
		value := structExtract.Field(i)

		var data string
		var err error
		switch class.String() {
		case "net.IP":
			//Currently provision is made only for IPV4
			//Following converts net.IP to string representation
			//Reflect calls net.IP.String() method to get string representation
			reflectValueFormatIPV4 := value.MethodByName("String").Call(nil)[0]
			stringIPV4 := reflectValueFormatIPV4.String()
			data, err = CompressIPV4(stringIPV4)
                case "[16]uint8":
			//Reflect value format is converted to interface then to [16]uint8 array
                        byteArray := value.Interface().([16]uint8)
			//Then [16]uint8 array is converted to uint8 slice and then to string
                        data = string(byteArray[:])
                case "uint8":
			//uint8 datatype is directly converted to string
                        data = string(value.Uint())
		case "uint16":
			//Following does coversion of uint16 from base 10 to base 256
			tempByteArray := make([]byte,2)
			binary.LittleEndian.PutUint16(tempByteArray, uint16(value.Uint()))
			data = string(tempByteArray)
		}

		if err != nil {
			return compressedOutput, err
		}
		compressedOutput += data
	}
	return compressedOutput, nil
}


/*
Func name : extractBytes
Description : Extracts the child byte slice from the specified position from
the parent string. It increments the offset, so that it points to next field's
start position.
*/
func extractBytes(data string, offset *int, size int) []byte {
	var returnBytes []byte
	returnBytes = []byte(data[*offset : *offset+size])
	*offset = *offset + size
	return returnBytes
}

/*
Func name : DecompressStructure
Description : Decompress the string data to structure data
*/
func DecompressStructure(StructData interface{}, compressedData string) {
	//Convert the structure passed to reflect value
	structExtract := reflect.ValueOf(StructData).Elem()
	offset := 0

	for i := 0; i < structExtract.NumField(); i++ {
		class := structExtract.Field(i).Type()
		var data interface{}
		switch class.String() {
		case "net.IP":
                        fieldValueBytes := extractBytes(compressedData, &offset, net.IPv4len)
                        stringIP := DecompressIPV4(string(fieldValueBytes))
                        data = net.ParseIP(stringIP)
                case "[16]uint8":
                        fieldValueBytes := extractBytes(compressedData, &offset, int(class.Size()))
			var array [16]uint8
                        copy(array[:], fieldValueBytes)
                        data = array
		case "uint8":
			fieldValueBytes := extractBytes(compressedData, &offset, int(class.Size()))
			data = fieldValueBytes[0]
		case "uint16":
			fieldValueBytes := extractBytes(compressedData, &offset, int(class.Size()))
			data = binary.LittleEndian.Uint16(fieldValueBytes)
		}

		//Fill the field
		//Coversion if interface type to field type
		value := reflect.ValueOf(data).Convert(class)
		field := structExtract.FieldByName(structExtract.Type().Field(i).Name)
		//Set field value
		if field.CanSet() {
			field.Set(value)
		}
	}
}

/*
Func name : CompressUUID
Description : Compress 36bytes string representation of UUID to
16bytes string
*/
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

