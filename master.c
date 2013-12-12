#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/shm.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <pthread.h>

typedef struct word_key{
 	char *word;
	int count;
	struct word_key *nextPtr;
} Word_key;

void map_job_scheduler(int num_map);
void red_job_scheduler (int num_map, int num_red);
void add_map_to_shm( Word_key *result,int job_number);
void add_red_to_shm( Word_key *result);
Word_key* mergeSort(Word_key *final_list);
Word_key* Final_merge(Word_key *first, Word_key *second);
void Split(Word_key *source, Word_key **front, Word_key **back);
void write_result_to_file(Word_key *final_list, char type);
void map();
void reduce();

char IN_FILE[30];
char OUT_FILE[30];
int map_schedule[100];
int red_schedule[100];
int job_num;
int num_red;
int SHMSZ;

pthread_mutex_t map_lock, red_lock;


void map_job_scheduler(int maps){

	int i;
	int f_size;
	int f_chunk_size;
	char c;
	FILE *fp;
	map_schedule[0]=0;
  	if( (fp=fopen(IN_FILE,"r")) == NULL){
	  printf("does not exist\n");
	  exit(1);
	}
	else{
	/* get file size*/
	  fseek(fp, 0, SEEK_END);
	  f_size = ftell(fp);
	  //printf(" fp position before => %d char %c\n",ftell(fp),fgetc(fp));
		
	  /* divide file among processes/threads */
	  f_chunk_size=f_size/maps;
	  //printf(" file size: %d chunk size => %d \n",f_size,f_chunk_size);
	
	/* set shared memory size to size of file*/
	  SHMSZ=f_chunk_size;
	  
	  /* set ending point of job to end of line */
	  for(i=1;i<= maps;i++){
	    rewind(fp);
	    //	printf("rewind position => %d\n",ftell(fp));
		
	    fseek(fp,i*f_chunk_size-1,SEEK_CUR);
		
	    //printf(" fp position before => %d\n",ftell(fp));
	    //printf(" fp position char => %c\n",fgetc(fp));
		
	    while((c=fgetc(fp)) != '\n'){
	      //		printf(" while loop fp position char => %c\n",c);
		
	    }
	    //printf(" fp position  at end of line => %d\n",ftell(fp));
	    map_schedule[i]=(int) ftell(fp);
	  }
	
	fclose(fp);
	}
	
}

void red_job_schedular(int num_maps,int num_red){

	int job_chunk;
	job_chunk= num_maps/num_red;
	int i;
	for(i=0 ; i < num_red ;i++){
		red_schedule[i] = job_chunk*i; 
	
	}
	red_schedule[num_red]=num_maps;

}

void add_map_to_shm(Word_key *result, int job_number){

  //printf("in add_map\n");
  Word_key **filemap,**shm_ptr;
  int shmid;
  const char *shm_name="maps";
  
  pthread_mutex_lock (&map_lock);

	/*open shared memory segment.*/ 
  if((shmid = shm_open(shm_name,O_RDWR, 0666)) < 0){
    printf("shared memory open failed\n");
    exit(1);
  }
  
  /* map shared memory to object*/
  if((filemap=mmap(0,sizeof(Word_key),PROT_READ|PROT_WRITE,MAP_SHARED,shmid,0)) == MAP_FAILED){
    printf("mapping failed\n");
    exit(1);
  }
  
  shm_ptr = filemap;
    
    while(1){
		//printf("was in add loop\n");
		if(*shm_ptr == NULL){
		//	printf("was in loop check\n");
			break;	
		}
		shm_ptr++;
 	}
	*shm_ptr=result;

  pthread_mutex_unlock (&map_lock);
}

void add_red_to_shm(Word_key *result){
	printf("in add_red\n");
  Word_key *filemap, *shm_ptr,*iterator_ptr;
  int shmid;
  const char *red_shm_name="red";
  
  pthread_mutex_lock(&red_lock);
  
  /* open shared memory segment. */
  if((shmid = shm_open(red_shm_name,O_CREAT | O_RDWR, 0666)) == -1){
    printf("shared memory open failed");
    exit(1);
  }
  
  /* map shared memory to object*/
  filemap=(Word_key *)mmap(0,sizeof(Word_key),PROT_WRITE,MAP_SHARED,shmid,0);
  
  shm_ptr = filemap;
  
  for(iterator_ptr=result;iterator_ptr->nextPtr != NULL; iterator_ptr=iterator_ptr->nextPtr){
		printf("iterating in add_red_to_shm");
  }
  iterator_ptr=shm_ptr;
  shm_ptr=iterator_ptr;
  pthread_mutex_unlock(&red_lock);

}

Word_key* mergeSort(Word_key *final_list){
  Word_key *head = final_list;
  Word_key *first, *second;
  
  if ((head == NULL) || (head->nextPtr == NULL)){
    return;
  }
  
  Split(head, &first, &second);
  mergeSort(&first);
  mergeSort(&second);

  head = Final_merge(first, second);
  return(head);
}

void Split(Word_key *source, Word_key **first, Word_key **second){
  Word_key *a, *b;
  if (source == NULL || source->nextPtr == NULL){
    *first = source;
    *second = NULL;
  }
  else{
    a = source;
    b = source->nextPtr;
    while (b != NULL){
      b = b->nextPtr;
      if (b != NULL){
	a = a->nextPtr;
	b = b->nextPtr;
      }
    }
    *first = source;
    *second = a->nextPtr;
    a->nextPtr = NULL;
  }
}

Word_key* Final_merge(Word_key *a, Word_key *b){
  Word_key *out = NULL;
  if (a == NULL){
    return(b);
  }
  else if (b==NULL){
    return(a);
  }
  int i = 0;
  while(i != -1) {
    if (a->word[i] < b->word[i]){
      out = a;
      out->nextPtr = Final_merge(a->nextPtr, b);
      i = -1;
    }
    else if (a->word[i] > b->word[i]){
      out = b;
      out->nextPtr = Final_merge(a, b->nextPtr);
      i = -1;
    }
    else if ((a->word[i] == NULL) && (b->word[i] != NULL)){
      out = a;
      out->nextPtr = Final_merge(a->nextPtr, b);
      i = -1;
    }
    else if ((a->word[i] != NULL) && (b->word[i] == NULL)){
      out = b;
      out->nextPtr = Final_merge(a, b->nextPtr);
      i = -1;
    }
    else if ((a->word[i] == NULL) && (b->word[i] == NULL)){
      out = a;
      out->nextPtr = Final_merge(a->nextPtr, b);
      i = -1;
    }
    else {
      i++;
    }
  }
  return(out);
}

void write_result_to_file(Word_key *final_list, char type){

	FILE *fp;
  char *str;
  // printf("before file loop");
  if( (fp=fopen(OUT_FILE,"a+")) == NULL){
    printf("%s does not exist\n",IN_FILE);
    exit(1);
  }
  else{
		if(type =="wordcount"){// write both word and count
			while(final_list != NULL){
				fprintf(fp,final_list->word);
				fprintf(fp," ");
				fprintf(fp,final_list->count);
				fprintf(fp,"\n");
				final_list=final_list->nextPtr;
			}
		}
		else{
			while(final_list != NULL){ // write only word
				fprintf(fp,final_list->word);
				fprintf(fp,"\n");
				final_list=final_list->nextPtr;
			}
		
		}
	}

}

void map(){
  	
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
	
}

void reduce(){
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
}

int main(int args, char * argv[]){

 	 /* check for correct command line args
   	if(args !=15 || args !=10){
    printf("Please check number of arguments\n");
    return 1;
    }*/
    
  	Word_key *filemap,*shm,*final_output;
  	int map_shmid,red_shmid;
 	const char *map_shm_name="maps";
	const char *red_shm_name="red";
	char *type=argv[2];
  
  	/* connect file name to global file variable */
	strcpy(IN_FILE, argv[10]);
	
 	 // printf("file name: %s\n",IN_FILE);
 	strcpy(OUT_FILE, argv[12]);
 	
	/* get number mappers for process/threads */
	int num_map= atoi(argv[6]);
	
	/* get number reducers for process/threads */
  	int num_red= atoi(argv[8]);
  	int f_size,i ;
  	
  	/* run maps job scheduler*/
	map_job_scheduler(num_map);
	
	/* run reduce job scheduler*/
	red_job_scheduler(num_map,num_red);
	
	/* create maps shared memory segment. */
  	if((map_shmid = shm_open(map_shm_name,O_CREAT | O_RDWR, 0666)) <0){
		printf("map shared memory creat failed");
  	 	exit(1);
  	}
  	
  	/* set map shared memory size*/
  	ftruncate(map_shmid,SHMSZ);

  		 
  	 /* create reduce shared memory segment. */
  	 if((red_shmid = shm_open(red_shm_name,O_CREAT | O_RDWR, 0666)) <0){
  	 	printf("shared memory creat failed");
  	 	exit(1);
  	 }
  	 
  	 /* set reduce shared memory size*/
  	 ftruncate(red_shmid,SHMSZ);

	
	//printf("file size : %d chunk size :%d\n",f_size,filechunk_size);
	
  	/*check for process or thread implementation */
  	
  	if((strcmp(argv[4],"procs"))==0){ // do process implementation
  
 		//printf(" in process\n");
  		//printf("num procs: %d\n",num_map);
	  //printf("after map creator\n");
	  
  		/* map phase starts */
	  pid_t pid;
		for( i=0; i < num_map;i++){
    
	    	pid_t pid=fork();
	    	if((pid) == 0){
	    	  job_num=i;
	    	  //printf("start: %d end:%d\n",job_start_ptr,job_end_ptr);
	    	  map();
				//printf("fork fptr number %d\n",getpid());
				exit(0);
			}
	    	else if(pid<0){
	     		printf("There was a fork error in %d count\n",i);
	   		}
	    	else{
				wait(NULL);
	    	}
    	}/* map phase ends*/
    	
    	/* reduce phase starts */
		for( i=0; i < num_red;i++){
    
	    	pid_t pid=fork();
	    	if((pid) == 0){
	    	  job_num=i;
	    	  //printf("start: %d end:%d\n",job_start_ptr,job_end_ptr);
	    	  reduce();
				//printf("fork fptr number %d\n",getpid());
			}
	    	else if(pid<0){
	     		printf("There was a fork error in %d count\n",i);
	   		}
	    	else{
				wait(NULL);
	    	}
    	}/* reduce phase ends*/
   
  	}
 	else if ((strcmp(argv[4],"threads"))==0){ // do thread implementation
 		/* map phase */
		pthread_t map_thread_id[num_map];
   		int i, j;
   		for(i=0; i < num_map; i++){
   			job_num=i;
      		pthread_create( &map_thread_id[i], NULL, map, NULL );
   		}
 
   		for(j=0; j < num_map; j++){
     		pthread_join( map_thread_id[j], NULL);
  		 }
		
		/* reduce phase */
		pthread_t red_thread_id[num_red];
		
   		for(i=0; i < num_red; i++){
   			job_num=i;
      		pthread_create( &red_thread_id[i], NULL, reduce, NULL );
   		}
 		for(j=0; j < num_red; j++){
     		pthread_join( red_thread_id[j], NULL);
  		 }

 	}
 	
 	/* map shared memory to object*/
  	if((filemap=mmap(0,SHMSZ,PROT_READ|PROT_WRITE,MAP_SHARED,red_shmid,0)) == MAP_FAILED){
    	printf("mapping failed\n");
    	exit(1);
  	}
  	shm=filemap;
  	final_output=mergeSort(shm);
 	write_to_file(final_output,type);
 	
 	/* delete all shared memory created */
 	shm_unlink (map_shm_name);
 	shm_unlink (red_shm_name);
 	
	return 0;
}