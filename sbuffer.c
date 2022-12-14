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

typedef struct sbuffer_node {
    struct sbuffer_node* prev;
    sensor_data_t data;
    int id;
    bool isProcessed;
    bool isStored;
} sbuffer_node_t;

struct sbuffer {
    sbuffer_node_t* head;
    sbuffer_node_t* tail;
    sbuffer_node_t* toProcess;
    sbuffer_node_t* toStore;
    bool closed;
    //pthread_mutex_t mutex;
    pthread_rwlock_t  rwlock;
};

static sbuffer_node_t* create_node(const sensor_data_t* data) {
    static int node_counter = 0;
    sbuffer_node_t* node = malloc(sizeof(*node));
    *node = (sbuffer_node_t){
        .data = *data,
        .prev = NULL,
        .id = ++node_counter,
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
    //ASSERT_ELSE_PERROR(pthread_mutex_init(&buffer->mutex, NULL) == 0);
    ASSERT_ELSE_PERROR(pthread_rwlock_init(&buffer->rwlock, NULL) == 0);

    return buffer;
}

void sbuffer_destroy(sbuffer_t* buffer) {
    assert(buffer);
    // make sure it's empty
    assert(buffer->head == buffer->tail);
    //ASSERT_ELSE_PERROR(pthread_mutex_destroy(&buffer->mutex) == 0);
    ASSERT_ELSE_PERROR(pthread_rwlock_destroy(&buffer->rwlock) == 0);
    free(buffer);
}

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
    ASSERT_ELSE_PERROR(pthread_rwlock_rdlock(&buffer->rwlock) == 0);
    assert(buffer);
    bool hasDataToStore = buffer->toStore != NULL;       
    ASSERT_ELSE_PERROR(pthread_rwlock_unlock(&buffer->rwlock) == 0);
    return hasDataToStore;
}

bool sbuffer_has_data_to_process(sbuffer_t* buffer) {    
    ASSERT_ELSE_PERROR(pthread_rwlock_rdlock(&buffer->rwlock) == 0);
    assert(buffer);
    bool hasDataToProcess = buffer->toProcess != NULL;   
    ASSERT_ELSE_PERROR(pthread_rwlock_unlock(&buffer->rwlock) == 0);
    return hasDataToProcess;
}

int sbuffer_insert_first(sbuffer_t* buffer, sensor_data_t const* data) {
    ASSERT_ELSE_PERROR(pthread_rwlock_wrlock(&buffer->rwlock) == 0);
    
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

    ASSERT_ELSE_PERROR(pthread_rwlock_unlock(&buffer->rwlock) == 0);
    
    return SBUFFER_SUCCESS;
}

sensor_data_t sbuffer_remove_last(sbuffer_t* buffer) {
    ASSERT_ELSE_PERROR(pthread_rwlock_wrlock(&buffer->rwlock) == 0);

    assert(buffer);
    assert(buffer->head != NULL);

    sbuffer_node_t* removed_node = buffer->tail;
    assert(removed_node != NULL);
    if (removed_node == buffer->head) {
        buffer->head = NULL;
        assert(removed_node == buffer->tail);
    }
    buffer->tail = removed_node->prev;
    sensor_data_t ret = removed_node->data;
    free(removed_node);

    ASSERT_ELSE_PERROR(pthread_rwlock_unlock(&buffer->rwlock) == 0);
    return ret;
}

/*
sensor_data_t sbuffer_get_last(sbuffer_t* buffer) {
    ASSERT_ELSE_PERROR(pthread_mutex_lock(&buffer->mutex) == 0);
    
    assert(buffer);
    assert(buffer->head != NULL);
    assert(buffer->tail != NULL);

    buffer->tail->data.isProcessed = true;
    sensor_data_t ret = buffer->tail->data;
    
    ASSERT_ELSE_PERROR(pthread_mutex_unlock(&buffer->mutex) == 0);    
    return ret;
}
*/

sensor_data_t sbuffer_get_last_to_process(sbuffer_t* buffer) {
    ASSERT_ELSE_PERROR(pthread_rwlock_wrlock(&buffer->rwlock) == 0);
    
    assert(buffer);
    assert(buffer->head != NULL);
    assert(buffer->tail != NULL);    
    assert(buffer->toProcess != NULL);
    
    sensor_data_t ret = buffer->toProcess->data;
    
    printf("id to process: %d\n", buffer->toProcess->id);

    // indicate the node as processed
    buffer->toProcess->isProcessed = true;

    // move the 'toProcess' pointer
    buffer->toProcess = buffer->toProcess->prev;
    
    ASSERT_ELSE_PERROR(pthread_rwlock_unlock(&buffer->rwlock) == 0);
    return ret;
}

sensor_data_t sbuffer_get_last_to_store(sbuffer_t* buffer) {
    ASSERT_ELSE_PERROR(pthread_rwlock_wrlock(&buffer->rwlock) == 0);
    
    assert(buffer);
    assert(buffer->head != NULL);
    assert(buffer->tail != NULL);    
    assert(buffer->toStore != NULL);
    
    sensor_data_t ret = buffer->toStore->data;
    
    printf("id to store: %d\n", buffer->toStore->id);

    // indicate the node as stored
    buffer->toStore->isStored = true;
    
    // move the 'toStore' pointer
    buffer->toStore = buffer->toStore->prev;
    
    ASSERT_ELSE_PERROR(pthread_rwlock_unlock(&buffer->rwlock) == 0);
    return ret;
}

void sbuffer_close(sbuffer_t* buffer) {
    ASSERT_ELSE_PERROR(pthread_rwlock_wrlock(&buffer->rwlock) == 0);
    assert(buffer);
    buffer->closed = true;
    ASSERT_ELSE_PERROR(pthread_rwlock_unlock(&buffer->rwlock) == 0);
}
