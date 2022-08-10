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
#include <sys/stat.h>
#include <sys/types.h>

struct job;
struct rocksdb_compo;

struct api
{
  void (*init)(struct rocksdb_compo *, char *DBPath);
  void (*shutdown)(struct job *j);
  void (*do_ops)(struct job *);
  void* (*sync)(void *);
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
  rocksdb_options_t *options;
  rocksdb_writeoptions_t *writeoptions;
  rocksdb_readoptions_t *readoptions;
};

struct config_stuff
{
  int keysize, ops, valsize, read, sync;
  long sleeptime;
  char *keypre, *keybase;
  char *DBPath;
};
const char sync_key[]="synckey";
const int KEY_MAX =64000;
const int VAL_MAX =4000000;
struct epoll_event event={
  .data.u32= -1U,
  .events = EPOLLIN,
};

void rand_str(void *, size_t);
void compo_init(struct rocksdb_compo *c, char *DBPath);
void compo_shutdown(struct job *j);
void complete_ops(struct job *j);
void do_writes(struct job *j);
void do_reads(struct job *j);
struct config_stuff config(int argc, char **argv);
void make_key(struct job *j, int size_diff, int i);
void do_job(struct config_stuff conf);
void* sleep_mimic(void * arg);
void* do_sync(void * arg);
void startthreads(struct config_stuff conf);

int main(int argc, char **argv) {
  struct config_stuff conf=config(argc,argv);
  startthreads(conf);
  do_job(conf);
  return 0;
}
void startthreads(struct config_stuff conf){
  pthread_t sleepthread;
  void *p = malloc(sizeof(long));
  p=&conf.sleeptime;
  pthread_create(&sleepthread,NULL,sleep_mimic,p);
}

void* do_sync(void * job_void){
  char *err = NULL;
  struct job *j=(struct job *)job_void;
  rocksdb_t *db=j->compo->db;
  struct timespec sleeptime;
  sleeptime.tv_nsec=4000000;
  rocksdb_writeoptions_t *writeoptions_sync = rocksdb_writeoptions_create();
  rocksdb_writeoptions_set_sync(writeoptions_sync, 1);
  while (j->conf->sync)
  {
    struct timespec sync_start;
    clock_gettime(CLOCK_REALTIME,&sync_start);
    nanosleep(&sleeptime,NULL);
    char syncval[25];
    snprintf(syncval,25,"%ld",sync_start.tv_nsec);
    rocksdb_put(db, writeoptions_sync, sync_key, strlen(sync_key), syncval, strlen(syncval),&err);
    struct timespec sync_end;
    clock_gettime(CLOCK_REALTIME,&sync_end);
    assert(!err);
    long timediff;
    timediff=((sync_end.tv_sec-sync_start.tv_sec)*1000000000)+sync_end.tv_nsec-sync_start.tv_nsec;
    if(timediff>=1.1*sleeptime.tv_nsec){
      fprintf(stderr,"sync time: %ld \n",timediff);
    }
  }
  return NULL;
}

void* sleep_mimic(void* sleeptime){ 
  long stime,sleep_sec,sleep_nsec;
  stime = *(long *) sleeptime;
  if(stime>=1000000000){
    double sec=(double)stime/(double)1000000000;
    long intPart = (long) sec;
    long fracPart = 1000000000*sec - 1000000000*intPart;
    sleep_sec=intPart;
    sleep_nsec=fracPart;
  }else{
    sleep_sec=0;
    sleep_nsec=stime;
  }
  int epollfd=epoll_create1(0);
  if (epollfd == -1)
  {
    perror("epoll_create1");
    exit(EXIT_FAILURE);
  }
  int fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
  if (fd == -1)
  {
    perror("timerfd_create");
    exit(EXIT_FAILURE);
  }
  int rc = epoll_ctl(epollfd, EPOLL_CTL_ADD, fd, &event);
  if (rc == -1)
  {
    perror("epoll_ctl");
    exit(EXIT_FAILURE);
  }
  while (1)
  {
    struct itimerspec its = {0};
    its.it_value.tv_sec=sleep_sec;
    its.it_value.tv_nsec=sleep_nsec;
    struct timespec start_time,end_time;
    clock_gettime(CLOCK_REALTIME,&start_time);
    rc = timerfd_settime(fd, 0, &its, NULL);
    if (rc == -1)
    {
      perror("timerfd_settime");
      exit(EXIT_FAILURE);
    }
    rc = epoll_wait(epollfd,&event,1,-1);//-1 so epoll is infinite if timer doesn't send
    if (rc == -1)
    {
      perror("epoll_wait");
      exit(EXIT_FAILURE);
    }
    clock_gettime(CLOCK_REALTIME,&end_time);
    struct itimerspec outs;
    rc = timerfd_gettime(fd,&outs);
    if (rc == -1)
    {
      perror("timerfd_settime");
      exit(EXIT_FAILURE);
    }
    long timediff=((end_time.tv_sec-start_time.tv_sec)*1000000000)+end_time.tv_nsec-start_time.tv_nsec;
    if(timediff>=1.1*stime){
      fprintf(stderr,"sleep set time %ld %ld \n",its.it_value.tv_sec, its.it_value.tv_nsec);
      fprintf(stderr,"sleep timediff: %ld \n",timediff);
      fprintf(stderr,"sleep get time %ld %ld \n",outs.it_value.tv_sec, outs.it_value.tv_nsec);
    }
  }
  return NULL;
}

void do_job(struct config_stuff conf){
    struct rocksdb_compo compo;
    struct job j={
    .job_api.init=compo_init,
    .job_api.do_ops=complete_ops,
    .job_api.sync=do_sync,
    .job_api.shutdown=compo_shutdown,
    .compo=&compo,
  };
  j.conf=&conf;
  j.job_api.init(j.compo,j.conf->DBPath);
  if (conf.sync==1)
  {
    pthread_t syncthread;
    pthread_create(&syncthread,NULL,j.job_api.sync,&j);
  }
  j.job_api.do_ops(&j);
  j.job_api.shutdown(&j);
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

void compo_init(struct rocksdb_compo *c, char *DBPath){
  c->options = rocksdb_options_create();
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
  c->writeoptions = rocksdb_writeoptions_create();
  c->readoptions = rocksdb_readoptions_create();
}

void compo_shutdown(struct job *j){
  if (j->conf->sync==1)
  {
    j->conf->sync=0;
  }
  char *err = NULL;
  rocksdb_close(j->compo->db);
  j->compo->db = rocksdb_open(j->compo->options, j->conf->DBPath, &err);
  assert(!err);
  rocksdb_writeoptions_destroy(j->compo->writeoptions);
  rocksdb_readoptions_destroy(j->compo->readoptions);
  rocksdb_options_destroy(j->compo->options);
  rocksdb_close(j->compo->db);
}

struct config_stuff config(int argc, char **argv){
  int opt;
  char* p;
  char *help="-k key size -n number of ops -v value size -p key prefix -r do reads -s do syncs -f path to DB -t time for timerfd in nsec -h help";
  struct config_stuff conf={
    .keysize=100,
    .ops=1000,
    .valsize=1000,
    .read=0,
    .sync=0,
    .sleeptime=30000000,
    .keypre="key",
    .DBPath=NULL,
  };
  while ((opt = getopt (argc, argv, ":if:k:n:v:p:f:t:rhs")) != -1)
  {
    switch (opt)
      {
        case 'k':
          if (strtol(optarg, &p, 10) < KEY_MAX){
            conf.keysize = strtol(optarg, &p, 10);
          }
          break;
        case 'n':
          conf.ops = strtol(optarg, &p, 10);
          break;
        case 'v':
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
        case 's':
          conf.sync=1;
          break;
        case 'f':
          conf.DBPath=optarg;
          break;
        case 't':
          conf.sleeptime=strtol(optarg, &p, 10);
          break;
        case 'h':
          printf("help called: %s \n", help);
          exit(1);
        default:
          printf("unrecognized flag: %s \n", help);
          exit(1);
      }
  }
  if (conf.DBPath==NULL)
  {
    printf("-f file path is mandatory\n");
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
  free(j->key);
  free(j->value);
}

void do_writes(struct job *j){
  char *err = NULL;
  srand(time(NULL));
  int size_diff=j->conf->keysize-strlen(j->conf->keypre);
  for (int i = 0; i < j->conf->ops; i++)
  {
    make_key(j,size_diff,i);
    if (i%100==0)
    {
      printf("writes: %d / %d\r",i,j->conf->ops);
    }
    rand_str(j->value, j->conf->valsize);
    rocksdb_put(j->compo->db, j->compo->writeoptions, j->key, strlen(j->key), j->value, j->conf->valsize,
            &err); 
    assert(!err);
  }
  printf("writes all completed\n");
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
    if (i%100==0)
    {
      printf("reads: %d / %d\r",i,j->conf->ops);
    }
    free(returned_value);
  }
  printf("reads all completed\n");
}

void make_key(struct job *j, int size_diff, int i){
  snprintf (j->key,j->conf->keysize+1,"%s%0*d\n",j->conf->keypre, size_diff, i);
}