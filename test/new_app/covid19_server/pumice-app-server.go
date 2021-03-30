package main
import (
        "fmt"
        "unsafe"
        "flag"
        "pmdblib/goPmdb"
        "covidapp.com/covidapplib"
	//"github.com/satori/go.uuid"
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
        PumiceDB.Decode(input_buf, apply_covid, input_buf_sz)

        fmt.Println("Key passed by client: %s", apply_covid.Location)

	//length of key.
	len_of_key := len(apply_covid.Location)

	//Convert value to int type.
	TotalVaccinations := PumiceDB.GoIntToString(int(apply_covid.Total_vaccinations))
        PeopleVaccinated := PumiceDB.GoIntToString(int(apply_covid.People_vaccinated))
	covideData_values := apply_covid.Iso_code + TotalVaccinations + PeopleVaccinated
        fmt.Println(covideData_values)
        covideData_len := PumiceDB.GoStringLen(covideData_values)

	//Generate app_uuid.
        //app_id := uuid.NewV4().String()

        fmt.Println("Write the KeyValue by calling PmdbWriteKV")
        //Write word and frequency as value to Pmdb
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

	//Generate app_uuid.
        //app_id := uuid.NewV4().String()

        /* Pass the work as key to PmdbReadKV and get the value from pumicedb */
        read_kv := PumiceDB.PmdbReadKV(app_id, req_struct.Location, int64(key_len), colmfamily)
        fmt.Println("Read KV:", read_kv)

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

