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
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
//Bismillah AlRahman AlRaheem
void usage() { print_mr2_usage(); }

int main(int argc, char **argv) {
  // setup pipes
  // start mappers
  // start shuffler
  // start reducers
  // wait for everything to finish ->lol okay!
    
    if (argc != 7) {
        usage();
        return 0;
    }
    
    /* File descriptors for the overall input and output files */
    int input_file = open(argv[1], O_RDWR | O_CREAT | O_CLOEXEC, S_IRWXU);
    
    
    remove(argv[2]);
    int output_file = open(argv[2], O_RDWR | O_APPEND | O_CREAT | O_CLOEXEC,  S_IRWXU);
    
    //number of mappers to account for
    int num_mappers = atoi(argv[5]);
    
    //number of reducers to account for
    int num_reducers = atoi(argv[6]);
    
    /* 2D Array with a row for each mapper, and 2 columns ->one for each end of the pipe*/
    
    //Create all necessary pipes
    int split_to_map[num_mappers][2]; // splitters to mappers
    int map_to_red[2]; //mappers to shuffler!
    
    //Initializing all required pipes
    for (int i = 0; i < num_mappers; i++)
        pipe2(split_to_map[i], O_CLOEXEC);
    
    pipe2(map_to_red, O_CLOEXEC);
    
    
    //now we need to create all necessary FIFO special files
    //keep in mind that these are just file names!
    char* shuff_to_red[num_reducers + 2]; // -> These will NEED to be freed at the end of PROG
    shuff_to_red[0] = "./shuffler";
    //make 'em
    for (int i = 0; i < num_reducers ; i++) {
        
        asprintf(&shuff_to_red[i + 1], "./fifo_%d", i);
        //printf("%s\n", shuff_to_red[i]);
        remove(shuff_to_red[i + 1]);
        int res = mkfifo(shuff_to_red[i + 1], S_IRWXU);
        if (res == -1) {
            perror("mkfifo");
            return 2;
        }
        
    }
    shuff_to_red[num_reducers + 1] = NULL;
    
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
    
    
    
    
    /**  create the shuffler process **/
    
    pid_t shuffler = fork();
    if (!shuffler) {
        //chlild
        
        for (int i = 0; i < num_mappers; i++){
            close(split_to_map[i][0]);
            close(split_to_map[i][1]);
        }
        
        //make pipe map_to_red take the place of stdin
        dup2(map_to_red[0], 0);
    
        //close write end of the pipe so we can get an EOF character
        close(map_to_red[1]);
        
        execv("./shuffler", shuff_to_red);
        perror("Shuffler");
        return 0;
        
    }
    
    
    // back to the main process
    
    pid_t reducer_children[num_reducers];
    
    /** finally create the reducer processes **/
    int reducer_number = 0;
    child = -1;
    while (reducer_number < num_reducers) {
        child = fork();
        if (!child) break;
        
        reducer_children[reducer_number] = child;
        
        reducer_number++;
    }
    
    if (!child) {
        //This is a reducer process
        
        //close proper pipes
        for (int i = 0; i < num_mappers; i++){
            close(split_to_map[i][0]);
            close(split_to_map[i][1]);
        }
        //both of these do not matter for this process
        close(map_to_red[1]);
        close(map_to_red[0]);
        
        //get FIFO files
        char** file_names = shuff_to_red + 1;
        char* file_name = file_names[reducer_number];
        int in_fd = open(file_name, O_RDONLY | O_CLOEXEC);
        
        dup2(in_fd, 0);
        dup2(output_file, 1);
        
        //set stdout and stdin for proper output/input stream
        
        execl(argv[4], argv[4], (char*)NULL);
        perror("Reducer");
        return 0;
        
    }
    
    
    /** End reducer processes */
    
    // back to main process
    
    
    for (int i = 0; i < num_mappers; i++){
        close(split_to_map[i][0]);
        close(split_to_map[i][1]);
    }
    
    
    close(input_file);
    close(output_file);
    close(map_to_red[1]);
    close(map_to_red[0]);
    
    
    for (int i = 0; i < num_reducers; i++) {
        int reducer_status;
        waitpid(reducer_children[i], &reducer_status, 0 );
    }

    
    //free all the FIFO stuff for the reducers
    for (int i = 1; i < num_reducers + 1; i++){
        remove(shuff_to_red[i]);
        free(shuff_to_red[i]);
    }
    

    

}
