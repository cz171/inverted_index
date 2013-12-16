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
/* word key pointer*/
  Word_key *main_ptr;
  Word_key *wk_ptr;
  Word_key *search_ptr;
  Word_key *curr_ptr;	
  int job_number=job_num;
	
  int word_exist=1;
	
  /* get file pointer number*/
   //printf("Job number =>%d\n",job_num);
	
  int start_pos=map_schedule[job_number];
  int end_pos=map_schedule[job_number+1];
	
  //printf("%d got start number =>%u and end number =>%u\n",getpid(),start_pos, end_pos);
	
  FILE *fp;
  char *str;
  // printf("before file loop");
  if( (fp=fopen(IN_FILE,"r")) == NULL){
    printf("%s does not exist\n",IN_FILE);
    exit(1);
  }
  else{
    rewind(fp);
    fseek(fp,start_pos,SEEK_CUR);
    //printf("fptr pos =>%d\n",ftell(fp));
		
    while(ftell(fp) < end_pos){
      /* get a word from file */
      fscanf(fp,"%s",&str);
      //printf("word scanned =>%s\n",&str);
      word_exist = 1;
      search_ptr=main_ptr;
      while( search_ptr != NULL ){
	//printf("in loop with word: %s\n",search_ptr->word);
				
	if( str == search_ptr->word ){
	 // printf("word exists!!!!!\n");
	  word_exist=0;
	  search_ptr->count++;
	//  printf("word: [%s]  exist ! count:[%d]\n",&search_ptr->word,search_ptr->count);
	}
				
	search_ptr=search_ptr->nextPtr;
      }
			
      if(word_exist == 1){
	//printf("in new word\n");
	wk_ptr=malloc(sizeof(Word_key));
	wk_ptr->word=str;
	wk_ptr->count=1;
	//printf("here\n");	    
	if(main_ptr == NULL){
			    
	  //printf("in  NULL new word\n");
	  main_ptr = curr_ptr = wk_ptr;
	}
	else{
	  //printf("in next new word\n");
	  curr_ptr->nextPtr= wk_ptr;
	  curr_ptr= curr_ptr->nextPtr;	
	}
      }
      str=NULL;
				
    }
  }
  fclose(fp);
      
  free(wk_ptr);
    
 	add_map_to_shm(main_ptr,job_number);
	
	// to be completed


}


void red(void){
	int job_number=job_num;
	int start_pos=map_schedule[job_number];
  	int end_pos=map_schedule[job_number+1];
  	
  	printf("in reduce\n");
  	printf("%d got start number =>%u and end number =>%u\n",getpid(),start_pos, end_pos);
  	
  Word_key **filemap,**shm_ptr,**shm_ptr2;
  int shmid;
  const char *shm_name="maps";
  
  Word_key *main_list, *sub_list,*iterate_ptr;

	/*open shared memory segment.*/ 
  if((shmid = shm_open(shm_name,O_RDWR, 0666)) < 0){
    printf("shared memory open failed\n");
    exit(1);
  }
  
  /* map shared memory to object*/
  if((filemap=mmap(0,sizeof(Word_key),PROT_READ,MAP_SHARED,shmid,0)) == MAP_FAILED){
    printf("mapping failed\n");
    exit(1);
  }
  
    int loop1_count=0;
    int loop2_count;
    for(shm_ptr = filemap; shm_ptr != NULL;shm_ptr++){
    	printf("in shm_ptr loop with count: %d\n",loop1_count);
		if(loop1_count == start_pos){
			loop2_count=loop1_count;
			for( ; shm_ptr != NULL;shm_ptr++){
				printf("in shm_ptr2 loop with count: %d\n",loop2_count);
				if(loop2_count == end_pos){
					printf("shm_ptr2 BREAK!! with count: %d\n",loop2_count);
					break;
				}
				else{
					if(main_list == NULL){
						printf("main_list NULL with count: %d\n",loop1_count);
						main_list=*shm_ptr2;
					}
					else{
					
						if(shm_ptr == NULL){
							printf("shm_ptr2 null BREAK!! with count: %d\n",loop1_count);
							break;
						}
						else{
							sub_list=iterate_ptr=*shm_ptr;
							while(iterate_ptr->nextPtr != NULL){
								// countinue to check
								printf("was iterating \n");
								iterate_ptr=iterate_ptr->nextPtr;
							}
						
							iterate_ptr->nextPtr=main_list;
							main_list=sub_list;
						
						}
					}
				}
				loop2_count++;
			}
			
		}
		loop1_count++;
	}
	
	add_to_red_shm(main_list);
	// to be completed


}


int main(int argc, char *argv[]){

  	#pragma omp parallel private(data) \
  		reduction(+:total_num)
  	{
  		data=0;
  		
  		#pragma omp for
  		for(i=0;i<num_blocks;++i)
  		{	
  			//map the data to against the function
  			mapped_num+=calc_data(data_array[i];
  		}
  		//reduce to get the result
  		totol_num+=mapped_num;
  	}
  	printf("the total number of the searched word is %d",total_num);
  	
  	return 0;
	filename = argv[1];
	map();
	red();

	return 0;

}
