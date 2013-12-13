#include <stdio.h>
#include <stdlib.h>
#include <omp.h>

char *filename;
char **map_results;
int maps;

//allcate the threads
   	maps=omp_get_thread_num();

typedef struct word_key{
         char *word;
        int count;
        struct word_key *nextPtr;
} Word_key;

void map(void){

	// to be completed


}


void red(void){

	// to be completed


}


int main(int argc, char *argv[]){

   	
	filename = argv[1];
	map();
	red();

	return 0;

}
