package GoPmdb

/*
#include <raft/pumice_db.h>
extern void goApply(const struct raft_net_client_user_id *,
              const char *, size_t, void *, void *);
extern void goRead(const struct raft_net_client_user_id *,
             const char *, size_t, char *, size_t, void*);
void applyCgo(const struct raft_net_client_user_id *app_id,
              const char *input_buf, size_t input_bufsz, void *pmdb_handle,
              void *user_data) {
	goApply(app_id, input_buf, input_bufsz, pmdb_handle, user_data);
}

void readCgo(const struct raft_net_client_user_id *app_id,
             const char *request_buf, size_t request_bufsz, char *reply_buf,
             size_t reply_bufsz, void *user_data) {
	goRead(app_id, request_buf, request_bufsz, reply_buf, reply_bufsz,
           user_data);
}
*/
import "C"
