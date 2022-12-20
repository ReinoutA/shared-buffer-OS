/**
 * \author Mathieu Erbas
 */

#ifndef _GNU_SOURCE
    #define _GNU_SOURCE
#endif

#include "sbuffer.h"

#include "config.h"

#include <assert.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>

struct sbuffer_node {
    struct sbuffer_node* prev;
    sensor_data_t data;
    int id;
    bool isProcessed;
    bool isStored;
};

struct sbuffer {
    sbuffer_node_t* head;
    sbuffer_node_t* tail;
    sbuffer_node_t* toProcess;
    sbuffer_node_t* toStore;

    bool closed;    

    pthread_rwlock_t    rwlock;
    pthread_cond_t      dataToRemove;
    pthread_cond_t      new_Data_Available_Low_Priority;  
    pthread_cond_t      new_Data_Available_High_Priority;
    pthread_mutex_t     mutex;
};


// -------------------------- CREATION -------------------------------------------
static sbuffer_node_t* create_node(const sensor_data_t* data) {
    static int node_counter = 0;
    sbuffer_node_t* node = malloc(sizeof(*node));
    *node = (sbuffer_node_t){
        .data = *data,
        .prev = NULL,
        .id = ++node_counter,
        .isProcessed = false,
        .isStored = false,
    };
    return node;
}

sbuffer_t* sbuffer_create() {
    sbuffer_t* buffer = malloc(sizeof(sbuffer_t));
    // should never fail due to optimistic memory allocation
    assert(buffer != NULL);

    buffer->head = NULL;
    buffer->tail = NULL;
    buffer->closed = false;
    buffer->toProcess = NULL;
    buffer->toStore = NULL;
    ASSERT_ELSE_PERROR(pthread_rwlock_init(&buffer->rwlock, NULL) == 0);
    ASSERT_ELSE_PERROR(pthread_cond_init(&buffer->new_Data_Available_Low_Priority, NULL) == 0);
    ASSERT_ELSE_PERROR(pthread_cond_init(&buffer->new_Data_Available_High_Priority, NULL) == 0);
    ASSERT_ELSE_PERROR(pthread_cond_init(&buffer->dataToRemove, NULL) == 0);
    ASSERT_ELSE_PERROR(pthread_mutex_init(&buffer->mutex, NULL) == 0);
 
    return buffer;
}

// ----------------------------- CLOSE BUFFER --------------------------------------

void sbuffer_close(sbuffer_t* buffer) {
    ASSERT_ELSE_PERROR(pthread_rwlock_wrlock(&buffer->rwlock) == 0);
    assert(buffer);
    buffer->closed = true;
    ASSERT_ELSE_PERROR(pthread_rwlock_unlock(&buffer->rwlock) == 0);
}

// ------------------------------ DESTROYING --------------------------------------- 

void sbuffer_destroy(sbuffer_t* buffer) {
    assert(buffer);
    // make sure it's empty
    assert(buffer->head == buffer->tail);
    ASSERT_ELSE_PERROR(pthread_mutex_destroy(&buffer->mutex) == 0);
    ASSERT_ELSE_PERROR(pthread_rwlock_destroy(&buffer->rwlock) == 0);
    ASSERT_ELSE_PERROR(pthread_cond_destroy(&buffer->new_Data_Available_Low_Priority) == 0);
    ASSERT_ELSE_PERROR(pthread_cond_destroy(&buffer->new_Data_Available_High_Priority) == 0);
    ASSERT_ELSE_PERROR(pthread_cond_destroy(&buffer->dataToRemove) == 0);
    free(buffer);
}

void node_destroy(sbuffer_node_t* node) {
    assert(node);    
    free(node);
}

// ------------------------------- PREDICATES -----------------------------------------
bool sbuffer_is_empty(sbuffer_t* buffer) {
    // use write lock instead of read lock to avoid data race condition
    ASSERT_ELSE_PERROR(pthread_rwlock_wrlock(&buffer->rwlock) == 0);
    assert(buffer);
    bool isEmpty = buffer->head == NULL;
    ASSERT_ELSE_PERROR(pthread_rwlock_unlock(&buffer->rwlock) == 0);
    return isEmpty;
}

bool sbuffer_is_closed(sbuffer_t* buffer) {
    ASSERT_ELSE_PERROR(pthread_rwlock_rdlock(&buffer->rwlock) == 0);
    assert(buffer);
    bool isClosed = buffer->closed;
    ASSERT_ELSE_PERROR(pthread_rwlock_unlock(&buffer->rwlock) == 0);
    return isClosed;
}

bool sbuffer_has_data_to_store(sbuffer_t* buffer) {
    // create a time value, 10 seconds from now
    struct timespec timeValue;
    clock_gettime(CLOCK_REALTIME, &timeValue);
    timeValue.tv_sec += 10;

    assert(buffer);
    bool hasDataToStore = false;
    ASSERT_ELSE_PERROR(pthread_mutex_lock(&buffer->mutex) == 0);
    hasDataToStore = buffer->toStore != NULL;
    if (!hasDataToStore) {
        printf("nothing to store, wait\n");
        //ASSERT_ELSE_PERROR(pthread_cond_wait(&buffer->new_Data_Available_Low_Priority, &buffer->mutex) == 0);
        int errorValue = pthread_cond_timedwait(&buffer->new_Data_Available_Low_Priority, &buffer->mutex, &timeValue);
        ASSERT_ELSE_PERROR((errorValue == 0) || (errorValue == ETIMEDOUT));
        printf("check data to store\n");
        hasDataToStore = buffer->toStore != NULL;
    }
    ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);

    return hasDataToStore; 
}

bool sbuffer_has_data_to_process(sbuffer_t* buffer) {
    // create a time value, 10 seconds from now
    struct timespec timeValue;
    clock_gettime(CLOCK_REALTIME, &timeValue);
    timeValue.tv_sec += 10;

    assert(buffer);
    bool hasDataToProcess = false;
    ASSERT_ELSE_PERROR(pthread_mutex_lock(&buffer->mutex) == 0);
    hasDataToProcess = buffer->toProcess != NULL;
    if (!hasDataToProcess) {
        printf("nothing to process, wait\n");
        //ASSERT_ELSE_PERROR(pthread_cond_wait(&buffer->new_Data_Available_High_Priority, &buffer->mutex) == 0);
        int errorValue = pthread_cond_timedwait(&buffer->new_Data_Available_High_Priority, &buffer->mutex, &timeValue);
        ASSERT_ELSE_PERROR((errorValue == 0) || (errorValue == ETIMEDOUT));
        printf("check data to process\n");
        hasDataToProcess = buffer->toProcess != NULL;
    }
    ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
    return hasDataToProcess;

}

// ------------------------------ INSERTING -----------------------------------------

int sbuffer_insert_first(sbuffer_t* buffer, sensor_data_t const* data) {
    ASSERT_ELSE_PERROR(pthread_mutex_lock(&buffer->mutex) == 0);
    assert(buffer && data);
    if (buffer->closed)
        return SBUFFER_FAILURE;
    
    // create new node
    sbuffer_node_t* node = create_node(data);
    assert(node->prev == NULL);

    // insert it
    if (buffer->head != NULL)
        buffer->head->prev = node;
    buffer->head = node;
    if (buffer->tail == NULL)
        buffer->tail = node;

    if (buffer->toProcess == NULL)
        buffer->toProcess = node;
    
    if (buffer->toStore == NULL)
        buffer->toStore = node;
    
    printf("insert node id: %d\n", node->id);
    // Wake up all waiting high priority readers
    ASSERT_ELSE_PERROR(pthread_cond_broadcast(&buffer->new_Data_Available_High_Priority) == 0); 
    // Wake up all waiting low priority readers
    ASSERT_ELSE_PERROR(pthread_cond_broadcast(&buffer->new_Data_Available_Low_Priority) == 0); 
    ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
    return SBUFFER_SUCCESS;
}
// -------------------------------- REMOVING ---------------------------------------

void sbuffer_remove_node(sbuffer_t* buffer)
{
    ASSERT_ELSE_PERROR(pthread_mutex_lock(&buffer->mutex) == 0);
    sbuffer_node_t* remove_node = buffer->tail;
    if (    (buffer->tail != NULL)
        &&  (buffer->tail->isProcessed && buffer->tail->isStored))
    {
        if (buffer->tail == buffer->head) {
            buffer->head = NULL;
        }
        buffer->tail = remove_node->prev;
        printf("node id = %d - temperature = %g - WILL BE REMOVED\n", remove_node->id, remove_node->data.value);        
        node_destroy(remove_node);     
    }
    else {
        printf("buffer->tail = %p\n", buffer->tail);
    }

    ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0); 
}

// ---------------------------------- GETTERS -----------------------------------------

sensor_data_t sbuffer_get_last_to_process(sbuffer_t* buffer) {    
    ASSERT_ELSE_PERROR(pthread_mutex_lock(&buffer->mutex) == 0);
    sbuffer_node_t* previous_node = NULL;
    bool removeNode = false;

    assert(buffer);
    assert(buffer->head != NULL);
    assert(buffer->tail != NULL);    
    assert(buffer->toProcess != NULL);
    
    sensor_data_t ret = buffer->toProcess->data;
    
    printf("id to process: %d\n", buffer->toProcess->id);
    
    // indicate the node as processed
    buffer->toProcess->isProcessed = true;
    previous_node = buffer->toProcess->prev;

    // check if this node was already stored,
    // and remove it, if needed
    removeNode = buffer->toProcess->isStored;
    
    // move the 'toProcess' pointer
    buffer->toProcess = previous_node;
    ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);

    if (removeNode)
    {
       ASSERT_ELSE_PERROR(pthread_cond_broadcast(&buffer->dataToRemove) == 0);
    }
    
    return ret;
}

sensor_data_t sbuffer_get_last_to_store(sbuffer_t* buffer) {
    ASSERT_ELSE_PERROR(pthread_mutex_lock(&buffer->mutex) == 0);
    sbuffer_node_t* previous_node = NULL;
    bool removeNode = false;
    assert(buffer);
    assert(buffer->head != NULL);
    assert(buffer->tail != NULL);    
    assert(buffer->toStore != NULL);
    
    sensor_data_t ret = buffer->toStore->data;
    
    printf("id to store: %d\n", buffer->toStore->id);    
    
    // indicate this node as stored
    buffer->toStore->isStored = true;
    previous_node= buffer->toStore->prev;

    // check if this node was already processed,
    // and remove it, if needed
    removeNode = buffer->toStore->isProcessed;
      
    // move the 'toStore' pointer
    buffer->toStore = previous_node;
    ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0); 

    if (removeNode)
    {
       ASSERT_ELSE_PERROR(pthread_cond_broadcast(&buffer->dataToRemove) == 0);
    }
    
    return ret;
}

bool sbuffer_has_data_to_remove(sbuffer_t* buffer)
{
    // create a time value, 10 seconds from now
    struct timespec timeValue;
    clock_gettime(CLOCK_REALTIME, &timeValue);
    timeValue.tv_sec += 10;

    assert(buffer);
    bool hasDataToRemove = false;
    ASSERT_ELSE_PERROR(pthread_mutex_lock(&buffer->mutex) == 0);
    hasDataToRemove = (buffer->tail != NULL) && buffer->tail->isProcessed && buffer->tail->isStored;
    if (!hasDataToRemove) {
        printf("nothing to remove, wait\n");
        //ASSERT_ELSE_PERROR(pthread_cond_wait(&buffer->dataToRemove, &buffer->mutex) == 0);
        int errorValue = pthread_cond_timedwait(&buffer->dataToRemove, &buffer->mutex, &timeValue);
        ASSERT_ELSE_PERROR((errorValue == 0) || (errorValue == ETIMEDOUT));
        printf("check data to remove\n");
        hasDataToRemove = (buffer->tail != NULL) && buffer->tail->isProcessed && buffer->tail->isStored;
    }
    ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
    return hasDataToRemove;
}


