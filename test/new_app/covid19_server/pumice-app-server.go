package main
import (
        "fmt"
        "unsafe"
        "flag"
        "pmdblib/goPmdb"
        "covidapp.com/covidapplib"
	"strconv"
	"log"
)

var seqno = 0
var raft_uuid_go string
var peer_uuid_go string
// Use the default column family
var colmfamily = "PMDBTS_CF"

func covid19_apply(app_id unsafe.Pointer, input_buf unsafe.Pointer,
                        input_buf_sz int64, pmdb_handle unsafe.Pointer) {

        fmt.Println("Covid19_Data app server: Apply request received")

        /* Decode the input buffer into structure format */
        apply_covid := &CovidAppLib.Covid_app{}
	fmt.Println("Structure format:", apply_covid)

        PumiceDB.Decode(input_buf, apply_covid, input_buf_sz)

        fmt.Println("Key passed by client: %s", apply_covid.Location)

	//length of key.
	len_of_key := len(apply_covid.Location)

	TotalVaccinations := PumiceDB.GoIntToString(int(apply_covid.Total_vaccinations))
        PeopleVaccinated := PumiceDB.GoIntToString(int(apply_covid.People_vaccinated))

	//Merge the all values.
	covideData_values := apply_covid.Iso_code+" "+TotalVaccinations+" "+PeopleVaccinated

	//length of all values.
        covideData_len := PumiceDB.GoStringLen(covideData_values)

	var preValueTV string
        var preValuePV string

        //Lookup the key first
        prevResultTV := PumiceDB.PmdbLookupKey(apply_covid.Location, int64(len_of_key), preValueTV, colmfamily)
        prevResultPV := PumiceDB.PmdbLookupKey(apply_covid.Location, int64(len_of_key), preValuePV, colmfamily)

        if prevResultTV != "" {
                prevResultTV_int, _ := strconv.Atoi(prevResultTV)
		Total_vaccinations_int, _ := strconv.Atoi(TotalVaccinations)
                Total_vaccinations_int = Total_vaccinations_int + prevResultTV_int
        } else if prevResultPV != "" {
                prevResultPV_int, _ := strconv.Atoi(prevResultPV)
		People_vaccinated_int, _ := strconv.Atoi(PeopleVaccinated)
		People_vaccinated_int = People_vaccinated_int + prevResultPV_int
        } else {
                fmt.Println("Nothing to update values.")
        }

	fmt.Println("Current Values:", covideData_values)
	fmt.Println("Previous value of the TotalVaccinations:", prevResultTV)
	fmt.Println("Previous value of the PeopleVaccinated:", prevResultPV)

        fmt.Println("Write the KeyValue by calling PmdbWriteKV")
        PumiceDB.PmdbWriteKV(app_id, pmdb_handle, apply_covid.Location, int64(len_of_key), covideData_values,
                             int64(covideData_len), colmfamily)
}

func covid19_read(app_id unsafe.Pointer, request_buf unsafe.Pointer,
            request_bufsz int64, reply_buf unsafe.Pointer, reply_bufsz int64) int64 {

        fmt.Println("Covid19_Data App: Read request received")

        //Decode the request structure sent by client.
        req_struct := &CovidAppLib.Covid_app{}
        PumiceDB.Decode(request_buf, req_struct, request_bufsz)

        fmt.Println("Key passed by client: %s", req_struct.Location)

        key_len := len(req_struct.Location)
        fmt.Println("Key length: %d", key_len)

	/* Pass the work as key to PmdbReadKV and get the value from pumicedb */
        read_kv_result := PumiceDB.PmdbReadKV(app_id, req_struct.Location, int64(key_len), colmfamily)
        fmt.Println("Read KV:", read_kv_result)

	/* typecast the output to int */
        if read_kv_result != "" {
                value, err := strconv.Atoi(read_kv_result)
                if err != nil {
                        log.Fatal(err)
                }
                fmt.Println("Value of the Key is: ", value)
        }

        //Copy the result in reply_buf
        reply_covid := (*CovidAppLib.Covid_app)(reply_buf)
        reply_covid.Location = req_struct.Location

        fmt.Println("Key: ", reply_covid.Location)

	return request_bufsz
}

func pmdb_dict_app_getopts() {

        flag.StringVar(&raft_uuid_go, "raft", "NULL", "raft uuid")
        flag.StringVar(&peer_uuid_go, "peer", "NULL", "peer uuid")

        flag.Parse()
        fmt.Println("Raft UUID: ", raft_uuid_go)
        fmt.Println("Peer UUID: ", peer_uuid_go)
}

func main() {
        //Parse the cmdline parameters
        pmdb_dict_app_getopts()

        //Initialize the dictionary application callback functions
        cb := &PumiceDB.PmdbCallbacks{
                ApplyCb: covid19_apply,
                ReadCb:  covid19_read,
        }

        PumiceDB.PmdbStartServer(raft_uuid_go, peer_uuid_go, colmfamily, cb)
}

