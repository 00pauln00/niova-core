package PumiceDBServer

/*
#include <raft/pumice_db.h>
extern int goWritePrep(const struct raft_net_client_user_id *,
                      const char *, size_t, void *, int *);
extern int goApply(const struct raft_net_client_user_id *,
              const char *, size_t, char *, size_t, void *, void *);
extern size_t goRead(const struct raft_net_client_user_id *,
             const char *, size_t, char *, size_t, void*);
int writePrepCgo(const struct raft_net_client_user_id *app_id,
                 const char *input_buf, size_t input_bufsz,
                 void *user_data, int *continue_wr) {
	return goWritePrep(app_id, input_buf, input_bufsz, user_data,
                       continue_wr);
}

int applyCgo(const struct raft_net_client_user_id *app_id,
              const char *input_buf, size_t input_bufsz, char *reply_buf,
              size_t reply_bufsz, void *pmdb_handle, void *user_data) {
	return goApply(app_id, input_buf, input_bufsz, reply_buf, reply_bufsz,
              pmdb_handle, user_data);
}

size_t readCgo(const struct raft_net_client_user_id *app_id,
             const char *request_buf, size_t request_bufsz, char *reply_buf,
             size_t reply_bufsz, void *user_data) {
	return goRead(app_id, request_buf, request_bufsz, reply_buf, reply_bufsz,
           user_data);
}
*/
import "C"
