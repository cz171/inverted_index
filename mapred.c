#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <omp.h>

char **fileList;
char *resultFile= "./result";
int num_to_reduce;

/* structures of each word*/
typedef struct word_key{
         char *word;
        char *docName;
} Word_key;

typedef struct comb{
         char *word;
        char **docName;
} comb_wordkey;

/* divide file into segments for mapping*/
int *run_Scheduler(int rank,int num_procs,char *file){
	int start,end;
	int f_size, job_sz;
	char c;
	FILE *fp;
	int *job= malloc(2*sizeof(int));

	fp=fopen(file,"r");
  	if(fp == NULL){
	  printf("rank=>%d,file does not exist\n",rank);
	  //exit(1);
	}
	else{
	/* get file size*/
	  fseek(fp, 0, SEEK_END);
	  f_size = ftell(fp);

	  job_sz=f_size/num_procs; 
	  rewind(fp);
	/*set start point*/
	  start= rank*job_sz;
	if(start ==0){
	 job[0]= start;
	}
	else{
	  fseek(fp,start-1,SEEK_CUR);
          while((c=fgetc(fp)) != '\n');// loop tile end of line   
	  start=ftell(fp);
	  job[0]=start+1;
	}
	rewind(fp);
	  /*set ending point*/
	    end= start + job_sz;
	    if(end >= f_size){
	      job[1] = f_size;
	    }
	    else {
	    fseek(fp,end-1,SEEK_CUR);
	     while((c=fgetc(fp)) != '\n');// loop tile end of line    
	     end = ftell(fp);
	     job[1]=end; 
	    }
	  }
	fclose(fp);
	//printf("my start=> %d, my end=>%d",job[0],job[1]);
	return job;

}

/*map words in segments of file*/
void map(int rank,int num_procs,int num_args){
	int i;
	int *my_job;
	FILE *fp;
	char *str;
	char *currFile=NULL;
	int count =0;
	Word_key *map_result;
	my_job= malloc(2*sizeof(int));

	/* allocate memory assuming 500 word each segment*/
	map_result= malloc((num_args*(500))*sizeof(Word_key));

	for(i=0;i < num_args-1;i++){
                currFile= fileList[i];
                my_job=run_Scheduler(rank,num_procs,currFile);	

	fp=fopen(currFile,"r");
        if(fp == NULL){
          printf("rank=>%d,could not open file %s\n",rank,currFile);
         
        }
	else{
	rewind(fp);
    	fseek(fp,my_job[0],SEEK_CUR);
	
	while(ftell(fp) < my_job[1]){// loop till end of job
	/* get next word */
      	fscanf(fp,"%s",&str);

	/* add word to result*/
	map_result[count].word=str;
	map_result[count].docName=currFile;
	
	/*advance iterator*/
	count++;
	}
	
	}
	}
		
	if(my_rank ==0){
	  num_per_reducer= comm_sz/2;
	}
	/*send to reducer*/

	if(comm_sz ==2){
	

	}	

}

void reduce(int my_rank,int comm_sz){
	int i;
	comb_wordkey *reduce_results;
	Word_key *map_result;
	map_result= malloc((num_args*(500))*sizeof(Word_key));

	/* combine maps */
	if(my_rank == 0){
	for(i=0;i<num_per_reducer;i++){

	}
	}

	if(my_rank ==1){
	for(i=number_per_reducer;i<comm_sz;i++){

        }

	}
	
	/*final combine and sort*/
	if(my_rank == 0){


	}

}

int main(int argc, char *argv[]){

	int i,thread_count;

	fileList= malloc((argc-1)*sizeof(char));
	/*check for filenames in cmd input*/	
	if( argc == 1 ){
		printf("Please enter files names\n");
		return -1;
	}
	/*create array for documents*/
	for(i=0 ; i < argc-1 ; i++){
		fileList[i] = argv[i+1];
	}
	thread_count = strtol(argv[1], NULL,10);	
	//map(my_rank,comm_sz,argc);
	//reduce(my_rank);

	return 0;
}

