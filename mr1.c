/**
 * MapReduce
 * CS 241 - Fall 2016
 */

#include "common.h"
#include "utils.h"
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

void usage() { print_mr1_usage(); }



int main(int argc, char **argv) {
    
    
    if (argc != 6) {
        usage();
        return 0;
    }
    
    /* File descriptors for the overall input and output files */
    int input_file = open(argv[1], O_RDWR | O_CREAT | O_CLOEXEC,   S_IRWXU);
    int output_file = open(argv[2], O_RDWR | O_TRUNC | O_CREAT | O_CLOEXEC,  S_IRWXU);
    
    //number of mappers to account for
    int num_mappers = atoi(argv[5]);
     /* 2D Array with a row for each mapper, and 2 columns ->one for each end of the pipe*/
     //Create all necessary pipes
    int split_to_map[num_mappers][2];
    int map_to_red[2];
    
    //Initializing all required pipes
    for (int i = 0; i < num_mappers; i++)
        pipe2(split_to_map[i], O_CLOEXEC);
    
    pipe2(map_to_red, O_CLOEXEC);
    
    
    /** Start up splitters **/
    int splitter_number = 0;
    pid_t child;
    while (splitter_number < num_mappers) {
        child = fork();
        if (!child) break;
        splitter_number++;
    }
    
    if (!child) {
        //this is a splitter process
        
        //stuff to pass into exec
        char* str_num_mappers;
        char* str_splitter_number;
        asprintf(&str_num_mappers, "%d", num_mappers);
        asprintf(&str_splitter_number, "%d", splitter_number);
        //set the stdout fd to the write pipe to a mapper
        dup2(split_to_map[splitter_number][1], 1);
        
        //close pipes not being used
        close(map_to_red[0]);
        close(map_to_red[1]);
        close(split_to_map[splitter_number][0]);
        
        execl("splitter", "splitter", argv[1], str_num_mappers, str_splitter_number, (char*)NULL);
        perror("Splitter");
        return 0;
        
    }
    
    /** Start up mappers **/
    int mapper_number = 0;
    child = -1;
    while (mapper_number < num_mappers) {
        child = fork();
        if (!child) break;
        mapper_number++;
    }
    
    if (!child) {
        //This is a mapper process
        
        //close proper pipes
        close(split_to_map[mapper_number][1]); //close write end of pipe from the mapper's end to receive EOF

        //set the stdout fd to the write pipe to a mapper
        dup2(split_to_map[mapper_number][0], 0); //get stdin from splitter's pipe
        dup2(map_to_red[1], 1); //write to reducer through pipe rather than through stdout
        execl(argv[3], argv[3], (char*)NULL);
        perror("Mapper");
        return 0;
        
    }
    
    //this is back to the main process -> close all file descriptors here
    
    
    /** Finally create the reducer process **/
    
    pid_t reducer = fork();
    if (!reducer) {
        //chlild
    
        for (int i = 0; i < num_mappers; i++){
            close(split_to_map[i][0]);
            close(split_to_map[i][1]);
        }
        
        //make pipe map_to_red take the place of stdin
        dup2(map_to_red[0], 0);
        //and the output file in place ofi stdout
        dup2(output_file, 1);
        //close proper pipes & files
        close(map_to_red[1]);
        
        execl(argv[4], argv[4], (char*)NULL);
        perror("Reducer");
        return 0;
        
    }
    
    
    //main process
    
    for (int i = 0; i < num_mappers; i++){
        close(split_to_map[i][0]);
        close(split_to_map[i][1]);
    }
    close(input_file);
    close(output_file);
    close(map_to_red[1]);
    close(map_to_red[0]);
    
    
    
    int reducer_status;
    waitpid(reducer, &reducer_status, 0 );
  

    
  // Create an input pipe for each mapper.

  // Create one input pipe for the reducer.

  // Open the output file.

  // Start a splitter process for each mapper.

  // Start all the mapper processes.

  // Start the reducer process.

  // Wait for the reducer to finish.

  // Print nonzero subprocess exit codes.

  // Count the number of lines in the output file.

    return 0;
}
