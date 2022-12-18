#pragma once

#ifndef _GNU_SOURCE
    #define _GNU_SOURCE
#endif

#include "config.h"
#include "lib/tcpsock.h"
#include "sbuffer.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/*
    This method holds the core functionality of the connmgr.
    It starts listening on the given port and when when a sensor
    node connects it writes the data to a sensor_data_recv file.
*/
void connmgr_listen(int port_number, sbuffer_t* buffer);
