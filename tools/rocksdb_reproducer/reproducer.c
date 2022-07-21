#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include<time.h>

#include "rocksdb/c.h"

#if defined(OS_WIN)
#include <Windows.h>
#else
#include <unistd.h>  // sysconf() - get CPU count
#endif

#if defined(OS_WIN)
const char DBPath[] = "C:\\Windows\\TEMP\\rocksdb_c_simple_example";
const char DBBackupPath[] =
    "C:\\Windows\\TEMP\\rocksdb_c_simple_example_backup";
#else
const char DBPath[] = "/tmp/rocksdb_c_simple_example";
const char DBBackupPath[] = "/tmp/rocksdb_c_simple_example_backup";
#endif

void rand_str(char *, size_t);

int main(int argc, char **argv) {
  rocksdb_t *db;
  rocksdb_backup_engine_t *be;
  rocksdb_options_t *options = rocksdb_options_create();
  // Optimize RocksDB. This is the easiest way to
  // get RocksDB to perform well.
#if defined(OS_WIN)
  SYSTEM_INFO system_info;
  GetSystemInfo(&system_info);
  long cpus = system_info.dwNumberOfProcessors;
#else
  long cpus = sysconf(_SC_NPROCESSORS_ONLN);
#endif
  // Set # of online cores
  rocksdb_options_increase_parallelism(options, (int)(cpus));
  rocksdb_options_optimize_level_style_compaction(options, 0);
  // create the DB if it's not already present
  rocksdb_options_set_create_if_missing(options, 1);

  // open DB
  char *err = NULL;
  db = rocksdb_open(options, DBPath, &err);
  assert(!err);

  // open Backup Engine that we will use for backing up our database
  be = rocksdb_backup_engine_open(options, DBBackupPath, &err);
  assert(!err);

  // Put key-value
  rocksdb_writeoptions_t *writeoptions = rocksdb_writeoptions_create();
  int size, i, n,valsize;
  if(argc != 5) {
    printf("usage: ./prog name age id_num gpa\n");
    exit(1);
  }
  char* p;
  size = strtol(argv[1], &p, 10);
  n = strtol(argv[2], &p, 10);
  valsize=strtol(argv[3], &p, 10);
  char keypre[strlen(argv[3])];
  strcpy(keypre,argv[4]);
  char buffer[size];
  memset(buffer,'0',size*sizeof(char));
  buffer[size]='\0';
  for (size_t i = 0; i < strlen(keypre); i++){
    buffer[i]=keypre[i];
  }
  printf("buffer: %s\n",buffer);
  char key[size];

  srand(time(NULL));
  // prepare key-value
  for (size_t i = 0; i < n; i++){
      char keypost[10] = {0};
      sprintf(keypost, "%0ld", i);
      if (size-strlen(keypost)>=strlen(keypre)){
        strcpy(key,buffer);
        key[strlen(key)-strlen(keypost)] = '\0';
      }else{
        strcpy(key,keypre);
      }
      strcat(key,keypost);
      printf("put key %s\n",key);
      char value[valsize+1];
      rand_str(value, sizeof value - 1);
      printf("put value %s\n",value);
      rocksdb_put(db, writeoptions, key, strlen(key), value, strlen(value) + 1,
              &err);
      assert(!err);
  }
  printf("out of loop\n");
  // Get value
  rocksdb_readoptions_t *readoptions = rocksdb_readoptions_create();
  size_t len;
  for (size_t i = 0; i < n; i++)
  {
      char keypost[10] = {0};
      sprintf(keypost, "%0ld", i);
      if (size-strlen(keypost)>=strlen(keypre)){
        strcpy(key,buffer);
        key[strlen(key)-strlen(keypost)] = '\0';
      }else{
        strcpy(key,keypre);
      }
      strcat(key,keypost);
      char add=i+'0';
      char *returned_value =rocksdb_get(db, readoptions, key, strlen(key), &len, &err);
      assert(!err);
  //assert(strcmp(returned_value, "value") == 0);
      printf("key %s\n",key);
      printf("value %s %ld\n",returned_value,strlen(returned_value));
      free(returned_value);
  }
  printf("backup 0\n");
  // create new backup in a directory specified by DBBackupPath
  rocksdb_backup_engine_create_new_backup(be, db, &err);
  assert(!err);

  rocksdb_close(db);
  printf("something wrong 0\n");
  // If something is wrong, you might want to restore data from last backup
  rocksdb_restore_options_t *restore_options = rocksdb_restore_options_create();
  rocksdb_backup_engine_restore_db_from_latest_backup(be, DBPath, DBPath,
                                                      restore_options, &err);
  assert(!err);
  rocksdb_restore_options_destroy(restore_options);

  db = rocksdb_open(options, DBPath, &err);
  assert(!err);
  printf("clean up\n");
  // cleanup
  rocksdb_writeoptions_destroy(writeoptions);
  rocksdb_readoptions_destroy(readoptions);
  rocksdb_options_destroy(options);
  rocksdb_backup_engine_close(be);
  rocksdb_close(db);
  printf("return 0\n");
  return 0;
}

void rand_str(char *dest, size_t length) {
    char charset[] = "0123456789"
                     "abcdefghijklmnopqrstuvwxyz"
                     "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    while (length-- > 0) {
        size_t index = (double) rand() / RAND_MAX * (sizeof charset - 1);
        *dest++ = charset[index];
    }
    *dest = '\0';
}
