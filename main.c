/**
 * \author Mathieu Erbas
 */

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

static pthread_mutex_t pthread_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t newData = PTHREAD_COND_INITIALIZER;
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

// static void* datamgr_run(void* buffer) {
//     datamgr_init();

//     // datamgr loop
//     while (true) {
//         sbuffer_lock(buffer);
    
//         if (!sbuffer_is_empty(buffer)) {
//             sensor_data_t data = sbuffer_remove_last(buffer);
//             datamgr_process_reading(&data);
//             // everything nice & processed
//         } else if (sbuffer_is_closed(buffer)) {
//             // buffer is both empty & closed: there will never be data again
//             sbuffer_unlock(buffer);
//             break;
//         }
//         // give the others a chance to lock the mutex
//         sbuffer_unlock(buffer);
//     }

//     datamgr_free();

//     return NULL;
// }

static void* datamgr_run(void* buffer) {  
   datamgr_init();

    // datamgr loop
    while (true) {        
        pthread_mutex_lock(&pthread_mutex);

        while (!sbuffer_has_data_to_process(buffer))
        {
            printf("nothing to process, wait\n");
            // sleep
            //nanosleep(&timeRequested500ms , &timeRemaining);
            //sleep(1);
            pthread_cond_wait(&newData, &pthread_mutex);
            printf("stop waiting to process\n");
        }

        sensor_data_t data = sbuffer_get_last_to_process(buffer);
        datamgr_process_reading(&data);
        printf("sensor id = %d - temperature = %g - PROCESSED\n", data.id, data.value);        

        pthread_mutex_unlock (&pthread_mutex);
    }

    datamgr_free();

    return NULL;
}

// static void* storagemgr_run(void* buffer) {
//     DBCONN* db = storagemgr_init_connection(1);
//     assert(db != NULL);

//     // storagemgr loop
//     while (true) {
//         sbuffer_lock(buffer);
//         if (!sbuffer_is_empty(buffer)) {
//             sensor_data_t data = sbuffer_remove_last(buffer);
//             storagemgr_insert_sensor(db, data.id, data.value, data.ts);
//             // everything nice & processed
//         } else if (sbuffer_is_closed(buffer)) {
//             // buffer is both empty & closed: there will never be data again
//             sbuffer_unlock(buffer);
//             break;
//         }
//         // give the others a chance to lock the mutex
//         sbuffer_unlock(buffer);
//     }

//     storagemgr_disconnect(db);
//     return NULL;
// }

static void* storagemgr_run(void* buffer) {
    DBCONN* db = storagemgr_init_connection(1);
    assert(db != NULL);

    // storagemgr loop
    while (true) {
        pthread_mutex_lock(&pthread_mutex);

        while (!sbuffer_has_data_to_store(buffer))
        {
            printf("nothing to store, wait\n");
            // sleep
            //nanosleep(&timeRequested500ms , &timeRemaining);
            //sleep(1);
            pthread_cond_wait(&newData, &pthread_mutex);
            printf("stop waiting to store\n");
        }
        
        sensor_data_t data = sbuffer_get_last_to_store(buffer);
        storagemgr_insert_sensor(db, data.id, data.value, data.ts);
        printf("sensor id = %d - temperature = %g - STORED\n", data.id, data.value);

        pthread_mutex_unlock (&pthread_mutex);
    }

    storagemgr_disconnect(db);
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

    pthread_t datamgr_thread;
    ASSERT_ELSE_PERROR(pthread_create(&datamgr_thread, NULL, datamgr_run, buffer) == 0);

    pthread_t storagemgr_thread;
    ASSERT_ELSE_PERROR(pthread_create(&storagemgr_thread, NULL, storagemgr_run, buffer) == 0);

    // main server loop
    connmgr_listen(port_number, buffer, &newData, &pthread_mutex);

    // first, check if all sbuffer data has been processed + sbuffer is empty
    while (!sbuffer_is_empty(buffer))
    {
        printf("connmgr_listen finished. Processing the remaining data\n");
        sleep(1);
    }

    // second, close the buffer
    sbuffer_close(buffer);

    pthread_join(datamgr_thread, NULL);
    pthread_join(storagemgr_thread, NULL);

    sbuffer_destroy(buffer);

    wait(NULL);

    return 0;
}