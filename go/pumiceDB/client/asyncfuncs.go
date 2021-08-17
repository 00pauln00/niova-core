package PumiceDBClient

/*
#include <raft/pumice_db.h>
extern void goAsyncCb(void *, ssize_t);
void asyncCbCgo(void *arg, ssize_t rrc) {
	goAsyncCb(arg, rrc);
}
*/
import "C"
