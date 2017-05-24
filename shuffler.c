/**
 * MapReduce
 * CS 241 - Fall 2016
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "common.h"
#include "utils.h"

void usage() {
  fprintf(stderr, "shuffler destination1 destination2 ...\n");
  fprintf(stderr, "where destination1..n are files on the filesystem to which "
                  "the shuffler will write its output\n");
}

int main(int argc, char *argv[]) {
  // read from stdin
  // hash the key for the input line
  // send them to the correct output file (output files are given as command
  // line arguments
    if (argc < 2) {
    usage();
    exit(1);
    }
    
    int num_files = argc -1;
    
    FILE* streams[num_files];
    char** file_names = argv + 1;
    int count = 0;
    while (*file_names) {
        streams[count] = fopen(*file_names, "w"); //read to write just for now so it doesnt block
        file_names++;
        count++;
    }
    
    file_names = argv + 1;
    
    char* buffer = NULL;
    size_t size = 0;
    int bytes_read = getline(&buffer, &size, stdin);
    
    while (bytes_read != -1) {
        
        char *key = NULL;
        char *value = NULL;
        split_key_value(buffer, &key, &value);

        FILE *outf = streams[ hashKey(key) % (num_files) ];
        fprintf(outf, "%s: %s\n", key, value);
        
        bytes_read = getline(&buffer, &size, stdin);
    }
    
    if (buffer) {
        free(buffer);
    }
    
    return 0;
}
