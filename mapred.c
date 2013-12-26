#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <omp.h>

char **fileList;
char *outputFile= "./output";
int num_per_reducer;
int num_args;


/* structures of each word*/
typedef struct word_key{
         char *word;
        char *docName;
} Word_key;

typedef struct comb{
         char *word;
        char **docName;
} comb_wordkey;

Word_key **map_data;
comb_wordkey *output;

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
	//printf("rank=>%d start=> %d, my end=>%d\n",rank,job[0],job[1]);
	return job;

}

/*map words in segments of file*/
void map(){
	int i;
	int *my_job;
	FILE *fp2;
	char *currFile=NULL;
	int count =0;
	int rank= omp_get_thread_num();
	int num_procs= omp_get_num_threads();
	char *line = NULL;
  	size_t len = 0;
  	ssize_t read;
  	char *tok;

	Word_key *map_result;
	my_job= malloc(2*sizeof(int));

	/* allocate memory assuming 500 word each segment*/
	map_result= malloc((num_args*(500))*sizeof(Word_key));

	for(i=0;i < num_args-2;i++){
	//printf("in mapping loop%d\n",num_args);
                currFile= fileList[i];
                my_job=run_Scheduler(rank,num_procs,currFile);	

	fp2=fopen(currFile,"r");
        if(fp2 == NULL){
          printf("rank=>%d,could not open file %s\n",rank,currFile);
         
        }else{
	rewind(fp2);
    	fseek(fp2,my_job[0],SEEK_CUR);

	while((read = getline(&line, &len, fp2)) != -1){// read line 
	/* get words */ 
	tok=strtok(line," ");
	
	while (tok != NULL){      	
	//printf("in tokens loop,%s, %s\n",tok,currFile);
	/* add word to result*/
	map_result[count].word=tok;
	map_result[count].docName=currFile;
	tok = strtok (NULL, " ");
	/*advance iterator*/
        count++;
	}
	if(ftell(fp2) >= my_job[1]) break;
	
	}
	
	}
	}
	#pragma omp critical
	{	
	map_data[rank]=map_result;
	//printf("rank %d added result to main\n",rank);	
	}
	
	if(rank ==0){
	  num_per_reducer= num_procs/2;
	}

	free(map_result);
	#pragma omp barrier
}

void reduce(){
	int a,i,j,k;
	int my_rank= omp_get_thread_num();
        int num_procs= omp_get_num_threads();
	comb_wordkey *reduce_result;
	Word_key *map_result;

	map_result= malloc((num_args*(500))*sizeof(Word_key));
	reduce_result=malloc((num_per_reducer*num_args*(500))*sizeof(Word_key));

	for(i=0;i<(num_per_reducer*num_args*(500));i++){
		reduce_result[i].docName=malloc((num_args-2)*sizeof(char));
	}

	/* combine maps */
	if(my_rank == 0){
	for(i=0;i<num_per_reducer;i++){
	printf("in map\n");
	map_result=map_data[i];
		for(j=0;j<(num_args*(500));j++){
		if(map_result[j].word != NULL){
		printf("in map-word\n");
		//printf("map word %s\n",map_result[j].word);
			for(k=0;k<(num_args*(500));k++){	
				if(reduce_result[k].word != NULL){
				printf("in result-word\n");
				//printf("reduce word %s\n",reduce_result[k].word);
				if(strcmp(reduce_result[k].word , map_result[j].word) == 0){
					/*for(a=0;a<num_args-2;a++){
					if(reduce_result[k].docName[a] != NULL){
					if(strcmp(reduce_result[k].docName[a],map_result[j].docName)==0)
						break;// word and docname exists
						printf("word+doc found\n");
					}else{
					reduce_result[k].docName[a]=map_result[j].docName;
						break;
						printf("added new docname\n");
					}
					}*/
				}

				}else{
					printf("in new word\n");
					//printf("new word, %s, added\n",map_result[j].word);
					reduce_result[k].word= map_result[j].word;
					reduce_result[k].docName[0]= map_result[j].docName;
				//	break;
				
				}	

			}

		}else{
			break;
	}
	}
	}
	}

/*
	if(my_rank ==1){
	for(i=num_per_reducer;i<num_procs;i++){
	map_result=map_data[i];
                for(j=0;j<sizeof(map_result);j++){
                if(map_result[j].word != NULL){
                        for(k=0;k<sizeof(comb_wordkey);k++){
                                if(reduce_result[k].word != NULL){
                                if(strcmp(reduce_result[k].word,map_result[j].word) == 0){
                                        for(a=0;a<num_args-2;a++){
                                        if(reduce_result[k].docName[a] != NULL){
                                        if(strcmp(reduce_result[k].docName[a],map_result[j].docName)==0)
                                                break;// word and docname exists
                                        }else{
                                        reduce_result[k].docName[a]=map_result[j].docName;
                                                break;
                                        }
                                        }
                                }

                                }else{
                                        reduce_result[k].word= map_result[j].word;
                                        reduce_result[k].docName[0]= map_result[j].docName;
                                        //break;

                                }

                        }

                }else{
                        break;
        }
        }
	

        }
	}*/
	#pragma omp barrier
	
	/* add to final list */
		
	
	/*final sort*/
	if(my_rank == 0){
	//sort();

	}

}

void sort(){

	// merge sort to be implemented


}

void write_output_to_file(){

	int j,i=0;
	FILE *fp;
	char *str= NULL;
	fp=fopen(outputFile,"w+");
        if(fp == NULL){
          printf("could not open file %s\n",outputFile);

        }else{
	/* build line message and write to file */
	while(output[i].word != NULL){

	sprintf(str,"%s ",output[i].word);
        write(fp,str,sizeof(str));

	for(j=0;j<num_args-2;j++){
	if(output[i].docName[j] == NULL) break;
	sprintf(str,"=>%s ",output[i].docName[j]);
	write(fp,str,sizeof(str));
	}
	sprintf(str,"\n");
        write(fp,str,sizeof(str));	
	i++;
	}
	
	}
}

int main(int argc, char *argv[]){

	int i,thread_count,count=2;
	num_args= argc;
	fileList= malloc((num_args-2)*sizeof(char));
	/*check for filenames in cmd input*/	
	if( num_args == 2 ){
		printf("Please enter files names\n");
		return -1;
	}
	/*create array for documents*/
	for(i=0 ; i < num_args-2 ; i++){
	//printf("file addistion loop\n");
		fileList[i] = argv[count];
		count++;
	}
	thread_count = strtol(argv[1], NULL,10);	
	map_data = malloc(thread_count*sizeof(Word_key));
	for(i=0;i<thread_count;i++){
	map_data[i]= malloc((num_args*(500))*sizeof(Word_key));
	}
	#pragma omp parallel num_threads(thread_count)
	map();

	#pragma omp parallel num_threads(2)
	reduce();


	//write_output_to_file();

	return 0;
}

