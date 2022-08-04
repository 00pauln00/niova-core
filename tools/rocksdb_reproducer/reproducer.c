#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <time.h>
#include <rocksdb/c.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/epoll.h>
#include <sys/timerfd.h>

struct job;
struct rocksdb_compo;

struct api
{
  void (*init)(struct rocksdb_compo *);
  void (*shutdown)(struct rocksdb_compo *);
  void (*do_ops)(struct job *);
};

struct job
{
  struct api job_api;
  struct config_stuff *conf;
  struct rocksdb_compo *compo;
  char *key;
  void *value;
};

struct rocksdb_compo
{
  rocksdb_t *db;
  rocksdb_backup_engine_t *be;
  rocksdb_options_t *options;
  rocksdb_writeoptions_t *writeoptions;
  rocksdb_readoptions_t *readoptions;
};

struct config_stuff
{
  int keysize, ops, valsize, read;
  char *keypre, *keybase;
};

const int KEY_MAX =64000;
const int VAL_MAX =4000000;

const char DBPath[] = "/tmp/rocksdb_c_simple_example";
const char DBBackupPath[] = "/tmp/rocksdb_c_simple_example_backup";


void rand_str(void *, size_t);
void compo_init(struct rocksdb_compo *c);
void compo_shutdown(struct rocksdb_compo *c);
void complete_ops(struct job *j);
void do_writes(struct job *j);
void do_reads(struct job *j);
struct config_stuff config(int argc, char **argv);
void make_key(struct job *j, int size_diff, int i);
void do_job(struct config_stuff conf);
void* sleep_mimic(void * arg);

int main(int argc, char **argv) {
  struct config_stuff conf;
  conf=config(argc,argv);
  pthread_t sleepthread;
  pthread_create(&sleepthread,NULL,sleep_mimic,NULL);
  do_job(conf);
  return 0;
}

void* sleep_mimic(void * arg){
  int fd = timerfd_create(CLOCK_REALTIME, 0);
  int i =1;
  while (1)
  {
    struct itimerspec its = {0};
    int set_returnval = timerfd_settime(fd, 0, &its, NULL);
    printf("set time %d %ld %ld %ld %ld \n", set_returnval, its.it_interval.tv_sec, its.it_interval.tv_nsec,its.it_value.tv_sec, its.it_value.tv_nsec);
    sleep(1);
    struct itimerspec outs;
    int get_returnval = timerfd_gettime(fd,&outs);
    printf("get time %d %ld %ld %ld %ld \n", get_returnval, outs.it_interval.tv_sec, outs.it_interval.tv_nsec,outs.it_value.tv_sec, outs.it_value.tv_nsec);
  }
  return NULL;
}

void do_job(struct config_stuff conf){
    struct rocksdb_compo compo;
    struct job j={
    .job_api.init=compo_init,
    .job_api.do_ops=complete_ops,
    .job_api.shutdown=compo_shutdown,
    .compo=&compo,
  };
  j.conf=&conf;
  j.job_api.init(j.compo);
  j.job_api.do_ops(&j);
  j.job_api.shutdown(j.compo);
}

void rand_str(void *dest, size_t length) {
    int *tmp=(int *)dest;
    int rem=length%4;
    int count=length-rem;
    for (size_t i = 0; i < count/4; i++)
    {
      tmp[i]=rand();
    }
    dest=tmp;
}

void compo_init(struct rocksdb_compo *c){
  c->options = rocksdb_options_create();
  // Optimize RocksDB. This is the easiest way to
  // get RocksDB to perform well.
  long cpus = sysconf(_SC_NPROCESSORS_ONLN);
  // Set # of online cores
  rocksdb_options_increase_parallelism(c->options, (int)(cpus));
  rocksdb_options_optimize_level_style_compaction(c->options, 0);
  // create the DB if it's not already present
  rocksdb_options_set_create_if_missing(c->options, 1);
  // open DB
  char *err = NULL;
  c->db = rocksdb_open(c->options, DBPath, &err);
  assert(!err);
  // open Backup Engine that we will use for backing up our database
  c->be = rocksdb_backup_engine_open(c->options, DBBackupPath, &err);
  assert(!err);
  c->writeoptions = rocksdb_writeoptions_create();
  c->readoptions = rocksdb_readoptions_create();
}

void compo_shutdown(struct rocksdb_compo *c){
  char *err = NULL;
  //rdb close and clean
  // create new backup in a directory specified by DBBackupPath
  rocksdb_backup_engine_create_new_backup(c->be, c->db, &err);
  assert(!err);

  rocksdb_close(c->db);
  // If something is wrong, you might want to restore data from last backup
  rocksdb_restore_options_t *restore_options = rocksdb_restore_options_create();
  rocksdb_backup_engine_restore_db_from_latest_backup(c->be, DBPath, DBPath,
                                                      restore_options, &err);
  assert(!err);
  rocksdb_restore_options_destroy(restore_options);

  c->db = rocksdb_open(c->options, DBPath, &err);
  assert(!err);
  // cleanup
  rocksdb_writeoptions_destroy(c->writeoptions);
  rocksdb_readoptions_destroy(c->readoptions);
  rocksdb_options_destroy(c->options);
  rocksdb_backup_engine_close(c->be);
  rocksdb_close(c->db);
  //rdb close and clean
}

struct config_stuff config(int argc, char **argv){
  int opt;
  char* p;
  char *help="-k key size -n number of ops -v value size -p key prefix -r do reads -h help";
  struct config_stuff conf;
  while ((opt = getopt (argc, argv, ":if:k:n:v:p:rh")) != -1)
    switch (opt)
      {
        case 'k':
        //compare this to MAX first
          if (strtol(optarg, &p, 10) < KEY_MAX){
            conf.keysize = strtol(optarg, &p, 10);
          }else{
            conf.keysize = KEY_MAX;
          }
          break;
        case 'n':
          conf.ops = strtol(optarg, &p, 10);
          break;
        case 'v':
        //compare this to MAX first
          if(strtol(optarg, &p, 10)<VAL_MAX){
            conf.valsize = strtol(optarg, &p, 10);
          }else
          {
            conf.valsize = VAL_MAX;
          }
          break;
        case 'p':
          conf.keypre=optarg;
          break;
        case 'r':
          conf.read=1;
          break;
        case 'h':
          printf("help called: %s \n", help);
          exit(1);
        default:
          printf("unrecognized flag: %s \n", help);
          exit(1);
      }
  return conf;
}

void complete_ops(struct job *j){
  j->key=(char*)malloc(j->conf->keysize+1);
  j->value= malloc(j->conf->valsize+1);
  do_writes(j);
  if (j->conf->read==1)
  {
    do_reads(j);
  }
  
}

void do_writes(struct job *j){
  char *err = NULL;
  srand(time(NULL));
  int size_diff=j->conf->keysize-strlen(j->conf->keypre);
  for (int i = 0; i < j->conf->ops; i++)
  {
    make_key(j,size_diff,i);
    //printf("j->key: %s\n",j->key);
    rand_str(j->value, j->conf->valsize);
    //printf("put j->value %p\n",j->value);
    rocksdb_put(j->compo->db, j->compo->writeoptions, j->key, strlen(j->key), j->value, j->conf->valsize,
            &err);
    //printf("after put\n");
    assert(!err);
  }
}

void do_reads(struct job *j){
  char *err = NULL;
  size_t len;
  int size_diff=j->conf->keysize-strlen(j->conf->keypre);
  for (int i = 0; i < j->conf->ops; i++)
  {
    make_key(j,size_diff,i);
    char *returned_value =rocksdb_get(j->compo->db, j->compo->readoptions, j->key, strlen(j->key), &len, &err);
    assert(!err);
    //printf("read key: %s\n",j->key);
    //printf("read value len: %ld\n",len);
    free(returned_value);
  }
}

void make_key(struct job *j, int size_diff, int i){
  snprintf (j->key,j->conf->keysize+1,"%s%0*d\n",j->conf->keypre, size_diff, i);
}