#ifndef _GNU_SOURCE
    #define _GNU_SOURCE
#endif

#include "config.h"
#include "connmgr.h"
#include "datamgr.h"
#include "sbuffer.h"
#include "sensor_db.h"

#include <assert.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <wait.h>

static bool threadCanRun = false;

static struct timespec timeRemaining;
static struct timespec timeRequested50ms = {
        0,               /* secs (Must be Non-Negative) */ 
       50 * 1000000     /* nano (Must be in range of 0 to 999999999) */ 
   };
static struct timespec timeRequested500ms = {
        0,               /* secs (Must be Non-Negative) */ 
       500 * 1000000     /* nano (Must be in range of 0 to 999999999) */ 
   };

static int print_usage() {
    printf("Usage: <command> <port number> \n");
    return -1;
}

static pthread_mutex_t threadCanRunMutex;

// get the run flag, in a safe way
static bool getThreadCanRun(void) {
    pthread_mutex_lock(&threadCanRunMutex);
    bool retValue = threadCanRun;
    pthread_mutex_unlock(&threadCanRunMutex);
    return retValue;
}

// set the run flag, in a safe way
static void setThreadCanRun(bool canRun) {
  pthread_mutex_lock(&threadCanRunMutex);
  threadCanRun = canRun;
  pthread_mutex_unlock(&threadCanRunMutex);
}

static void* datamgr_run(void* buffer) {  
   datamgr_init();

    // datamgr loop
    while (getThreadCanRun()) {        
        // datamgr waits on CV when no data is available to process
        if(sbuffer_has_data_to_process(buffer)){
            sensor_data_t data = sbuffer_get_last_to_process(buffer);
            datamgr_process_reading(&data);
            printf("sensor id = %d - temperature = %g - PROCESSED\n", data.id, data.value);        
            //nanosleep(&timeRequested500ms, &timeRemaining);
        }
    }
    
    datamgr_free();

    printf("shutdown datamgr_run thread\n");
    return NULL;
}

static void* storagemgr_run(void* buffer) {
    DBCONN* db = storagemgr_init_connection(1);
    assert(db != NULL);

    // storagemgr loop
    while (getThreadCanRun()) {
       // storagemgr waits on CV when no data is available to store
       if(sbuffer_has_data_to_store(buffer)){
            sensor_data_t data = sbuffer_get_last_to_store(buffer);
            storagemgr_insert_sensor(db, data.id, data.value, data.ts);
            printf("sensor id = %d - temperature = %g - STORED\n", data.id, data.value);
            //nanosleep(&timeRequested500ms, &timeRemaining);
        }
    }

    storagemgr_disconnect(db);

    printf("shutdown storagemgr_run thread\n");
    return NULL;
}

static void* removemgr_run(void* buffer) {  
    // removemgr loop
    while (getThreadCanRun()) {        
        // removemgr waits on CV when no data is available to process
        if(sbuffer_has_data_to_remove(buffer)){
            sbuffer_remove_node(buffer);
        }
    }
    
    printf("shutdown removemgr_run thread\n");
    return NULL;
}

int main(int argc, char* argv[]) {
    if (argc != 2)
        return print_usage();
    char* strport = argv[1];
    char* error_char = NULL;
    int port_number = strtol(strport, &error_char, 10);
    if (strport[0] == '\0' || error_char[0] != '\0')
        return print_usage();

    sbuffer_t* buffer = sbuffer_create();
    
    // set flag to indicate threads can run
    ASSERT_ELSE_PERROR(pthread_mutex_init(&threadCanRunMutex, NULL) == 0);
    setThreadCanRun(1);

    pthread_t datamgr_thread;
    pthread_t storagemgr_thread;
    pthread_t removemgr_thread;

    ASSERT_ELSE_PERROR(pthread_create(&datamgr_thread, NULL, datamgr_run, buffer) == 0);
    ASSERT_ELSE_PERROR(pthread_create(&storagemgr_thread, NULL, storagemgr_run, buffer) == 0);
    ASSERT_ELSE_PERROR(pthread_create(&removemgr_thread, NULL, removemgr_run, buffer) == 0);

    // main server loop
    connmgr_listen(port_number, buffer);

    // first, check if all sbuffer data has been processed + sbuffer is empty
    while (!sbuffer_is_empty(buffer))
    {
        printf("connmgr_listen finished. Processing the remaining data\n");
        sleep(1);
    }

    // set a flag to indicate that all threads can shutdown
    setThreadCanRun(false);
    printf("All sensor values have been handled, buffer is empty. All threads can stop running.\n");

    // second, close the buffer
    printf("Close the buffer\n");
    sbuffer_close(buffer);    

    printf("Shutting down threads in 10 seconds ...\n");
    pthread_join(storagemgr_thread, NULL);
    pthread_join(datamgr_thread, NULL);
    pthread_join(removemgr_thread, NULL);

    printf("Destroy the buffer\n");
    sbuffer_destroy(buffer);
    ASSERT_ELSE_PERROR(pthread_mutex_destroy(&threadCanRunMutex) == 0);

    wait(NULL);

    return 0;
}