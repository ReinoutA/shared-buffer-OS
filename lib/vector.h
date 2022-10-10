#pragma once

#ifndef _GNU_SOURCE
    #define _GNU_SOURCE
#endif

#include <stdbool.h>
#include <unistd.h>

typedef struct vector vector_t;

vector_t* vector_create();

void vector_add(vector_t* vec, void* element);

void vector_remove_at_index(vector_t* vec, size_t index);

void* vector_at(vector_t* vec, size_t index);

void* vector_find(vector_t* vec, void* element_to_match, bool (*match_elements)(void*, void*));

size_t vector_size(vector_t* vec);

void vector_destroy(vector_t* vec);
