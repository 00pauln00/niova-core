package PumiceDBServer

/*
#include <raft/pumice_db.h>
extern ssize_t goWritePrep(struct pumicedb_cb_cargs *, int *);
extern ssize_t goApply(struct pumicedb_cb_cargs *, void *);
extern ssize_t goRead(struct pumicedb_cb_cargs *);
extern void goInitLeader(struct pumicedb_cb_cargs *);

ssize_t writePrepCgo(struct pumicedb_cb_cargs * args, int *continue_wr) {
	return goWritePrep(args, continue_wr);
}

ssize_t applyCgo(struct pumicedb_cb_cargs * args, void *pmdb_handle) {
	return goApply(args, pmdb_handle);
}

ssize_t readCgo(struct pumicedb_cb_cargs *args) {
	return goRead(args);
}

void initLeaderCgo(struct pumicedb_cb_cargs *args) {
    return goInitLeader(args);
}
*/
import "C"
