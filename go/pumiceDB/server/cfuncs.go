package PumiceDBServer

/*
#include <raft/pumice_db.h>
extern ssize_t goWritePrep(struct pumicedb_cb_cargs *);
extern ssize_t goApply(struct pumicedb_cb_cargs *);
extern ssize_t goRead(struct pumicedb_cb_cargs *);
extern void goInitPeer(struct pumicedb_cb_cargs *);
extern void goCleanupPeer(struct pumicedb_cb_cargs *);

ssize_t writePrepCgo(struct pumicedb_cb_cargs * args) {
	return goWritePrep(args);
}

ssize_t applyCgo(struct pumicedb_cb_cargs * args) {
	return goApply(args);
}

ssize_t readCgo(struct pumicedb_cb_cargs *args) {
	return goRead(args);
}

void initPeerCgo(struct pumicedb_cb_cargs *args) {
    return goInitPeer(args);
}

void cleanupPeerCgo(struct pumicedb_cb_cargs *args) {
    return goCleanupPeer(args);
}
*/
import "C"
