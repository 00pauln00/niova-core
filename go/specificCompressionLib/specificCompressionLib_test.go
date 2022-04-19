package specificCompressionLib

import(
	"testing"
	"net"
	"github.com/stretchr/testify/assert"
)


type foo struct{
	Var1 uint8
	Var2 uint16
	Var3 uint16
}


type bar struct{
	Var1 net.IP
}



func TestCompression(t *testing.T){
	//Fill struct
	fooSample := foo{
		Var1 : 100,
		Var2 : 1020,
		Var3 : 50000,
	}

	//Compress the structure
	compressedString, err := CompressStructure(fooSample)
	if err != nil {
		assert.Error(t, err)
	}

	//Decompress the the string value
	fooSample2 := foo{}
	changedString := compressedString
	DecompressStructure(&fooSample2, changedString)
	//Compress it again and check it with previously created compressed string
	//If matches validates the correctness of the compression library
	checkCompressedString, err := CompressStructure(fooSample2)
	if err != nil {
                assert.Error(t, err)
        }
	assert.Equal(t, compressedString, checkCompressedString, "Compression/Decompression failure")
}
