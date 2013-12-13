#include <stdio.h>
#include <stdlib.h>
#include <omp.h>

char *filename;
char **map_results;
int maps;

void map(void){

	// to be completed


}


void red(void){

	// to be completed


}


int main(int argc, char *argv[]){
//allcate the threads
   	maps=omp_get_thread_num();
   	
	filename = argv[1];
	map();
	red();

	return 0;

}
