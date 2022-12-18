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
    pthread_mutex_t mutex;
};

struct sbuffer {
    sbuffer_node_t* head;
    sbuffer_node_t* tail;
    sbuffer_node_t* toProcess;
    sbuffer_node_t* toStore;
    bool closed;    
    pthread_rwlock_t    rwlock;
    pthread_cond_t      dataToRemove;
    pthread_cond_t      newDataAvailable;
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
    ASSERT_ELSE_PERROR(pthread_mutex_init(&node->mutex, NULL) == 0);
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
    ASSERT_ELSE_PERROR(pthread_cond_init(&buffer->newDataAvailable, NULL) == 0);
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
    ASSERT_ELSE_PERROR(pthread_cond_destroy(&buffer->newDataAvailable) == 0);
    ASSERT_ELSE_PERROR(pthread_cond_destroy(&buffer->dataToRemove) == 0);
    free(buffer);
}

void node_destroy(sbuffer_node_t* node) {
    assert(node);    
    ASSERT_ELSE_PERROR(pthread_mutex_destroy(&node->mutex) == 0);
    free(node);
}

// ------------------------------- PREDICATES -----------------------------------------
bool sbuffer_is_empty(sbuffer_t* buffer) {
    ASSERT_ELSE_PERROR(pthread_rwlock_rdlock(&buffer->rwlock) == 0);
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
    assert(buffer);
    bool hasDataToStore = false;
    ASSERT_ELSE_PERROR(pthread_mutex_lock(&buffer->mutex) == 0);
    hasDataToStore = buffer->toStore != NULL;
    if (!hasDataToStore) {
        printf("nothing to store, wait\n");
        ASSERT_ELSE_PERROR(pthread_cond_wait(&buffer->newDataAvailable, &buffer->mutex) == 0);
        printf("stop waiting to store\n");
        hasDataToStore = buffer->toStore != NULL;
    }
    ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);

    return hasDataToStore; 
}

bool sbuffer_has_data_to_process(sbuffer_t* buffer) {   
    assert(buffer);
    bool hasDataToProcess = false;
    ASSERT_ELSE_PERROR(pthread_mutex_lock(&buffer->mutex) == 0);
    hasDataToProcess = buffer->toProcess != NULL;
    if (!hasDataToProcess) {
        printf("nothing to process, wait\n");
        ASSERT_ELSE_PERROR(pthread_cond_wait(&buffer->newDataAvailable, &buffer->mutex) == 0);
        printf("stop waiting to process\n");
        hasDataToProcess = buffer->toProcess != NULL;
    }
    ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
    return hasDataToProcess;

}

// ------------------------------ INSERTING -----------------------------------------

int sbuffer_insert_first(sbuffer_t* buffer, sensor_data_t const* data) {
    //ASSERT_ELSE_PERROR(pthread_rwlock_wrlock(&buffer->rwlock) == 0);
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
    ASSERT_ELSE_PERROR(pthread_cond_broadcast(&buffer->newDataAvailable) == 0); // Wake up all waiting readers
    ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
    //ASSERT_ELSE_PERROR(pthread_rwlock_unlock(&buffer->rwlock) == 0);   
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
    //ASSERT_ELSE_PERROR(pthread_rwlock_rdlock(&buffer->rwlock) == 0);
    ASSERT_ELSE_PERROR(pthread_mutex_lock(&buffer->mutex) == 0);
    sbuffer_node_t* previous_node = NULL;
    bool removeNode = false;

    //ASSERT_ELSE_PERROR(pthread_mutex_lock(&buffer->toProcess->mutex) == 0);
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
    if (buffer->toProcess->isStored == true){
        removeNode = true;
    }
    
    //ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->toProcess->mutex) == 0);
    //ASSERT_ELSE_PERROR(pthread_rwlock_unlock(&buffer->rwlock) == 0);
    
    //ASSERT_ELSE_PERROR(pthread_rwlock_wrlock(&buffer->rwlock) == 0);
    // move the 'toProcess' pointer
    buffer->toProcess = previous_node;
    //ASSERT_ELSE_PERROR(pthread_rwlock_unlock(&buffer->rwlock) == 0);
    ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);

    if (removeNode)
    {
       ASSERT_ELSE_PERROR(pthread_cond_broadcast(&buffer->dataToRemove) == 0);
    }
    
    return ret;
}

sensor_data_t sbuffer_get_last_to_store(sbuffer_t* buffer) {
    //ASSERT_ELSE_PERROR(pthread_rwlock_rdlock(&buffer->rwlock) == 0);
    ASSERT_ELSE_PERROR(pthread_mutex_lock(&buffer->mutex) == 0);
    sbuffer_node_t* previous_node = NULL;
    bool removeNode = false;
    //ASSERT_ELSE_PERROR(pthread_mutex_lock(&buffer->toStore->mutex) == 0);

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
    if (buffer->toStore->isProcessed == true){
        removeNode = true;
    }
    
    //ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->toStore->mutex) == 0);    
    //ASSERT_ELSE_PERROR(pthread_rwlock_unlock(&buffer->rwlock) == 0);
    
    //ASSERT_ELSE_PERROR(pthread_rwlock_wrlock(&buffer->rwlock) == 0);    
    // move the 'toStore' pointer
    buffer->toStore = previous_node;
    //ASSERT_ELSE_PERROR(pthread_rwlock_unlock(&buffer->rwlock) == 0);
    ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0); 

    if (removeNode)
    {
       ASSERT_ELSE_PERROR(pthread_cond_broadcast(&buffer->dataToRemove) == 0);
    }
    
    return ret;
}

bool sbuffer_has_data_to_remove(sbuffer_t* buffer)
{
    assert(buffer);
    bool hasDataToRemove = false;
    ASSERT_ELSE_PERROR(pthread_mutex_lock(&buffer->mutex) == 0);
    hasDataToRemove = (buffer->tail != NULL) && buffer->tail->isProcessed && buffer->tail->isStored;
    if (!hasDataToRemove) {
        printf("nothing to remove, wait\n");
        ASSERT_ELSE_PERROR(pthread_cond_wait(&buffer->dataToRemove, &buffer->mutex) == 0);
        printf("stop waiting to remove\n");
        hasDataToRemove = (buffer->tail != NULL) && buffer->tail->isProcessed && buffer->tail->isStored;
    }
    ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);
    return hasDataToRemove;
}


// ------------------------------ GETTERS THAT LOCK ON BUFFER -----------------------------------------------

/*
sensor_data_t sbuffer_get_last_to_process(sbuffer_t* buffer) {
    sbuffer_node_t* remove_node = NULL;
    ASSERT_ELSE_PERROR(pthread_rwlock_wrlock(&buffer->rwlock) == 0);

    assert(buffer);
    assert(buffer->head != NULL);
    assert(buffer->tail != NULL);    
    assert(buffer->toProcess != NULL);
    
    sensor_data_t ret = buffer->toProcess->data;

    printf("id to process: %d\n", buffer->toProcess->id);

    
    // indicate the node as processed
    buffer->toProcess->isProcessed = true;

    // check if this node was already stored,
    // and remove it, if needed
    if (buffer->toProcess->isStored == true)
    {        
        remove_node = buffer->toProcess;
    }

    // move the 'toProcess' pointer
    buffer->toProcess = buffer->toProcess->prev;


    if(remove_node != NULL)
    {
       sbuffer_remove_node(buffer, remove_node);
    }
    ASSERT_ELSE_PERROR(pthread_rwlock_unlock(&buffer->rwlock) == 0);

    return ret;
}

sensor_data_t sbuffer_get_last_to_store(sbuffer_t* buffer) {
    sbuffer_node_t* remove_node = NULL;
    ASSERT_ELSE_PERROR(pthread_rwlock_wrlock(&buffer->rwlock) == 0);

    assert(buffer);
    assert(buffer->head != NULL);
    assert(buffer->tail != NULL);    
    assert(buffer->toStore != NULL);

    sensor_data_t ret = buffer->toStore->data;

    printf("id to store: %d\n", buffer->toStore->id);    

    // indicate this node as stored
    buffer->toStore->isStored = true;

    // check if this node was already processed,
    // and remove it, if needed
    if (buffer->toStore->isProcessed == true)
    {        
        remove_node = buffer->toStore;
    }

    // move the 'toStore' pointer
    buffer->toStore = buffer->toStore->prev;


    if(remove_node != NULL)
    {
       sbuffer_remove_node(buffer, remove_node);
    }
    ASSERT_ELSE_PERROR(pthread_rwlock_unlock(&buffer->rwlock) == 0);

    return ret;
}
*/