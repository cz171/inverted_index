#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/time.h>
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
  	char *tok=NULL;
	
	my_job= malloc(2*sizeof(int));

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
	tok=strtok(line," .,;:!-");

	while (tok != NULL){      	
	//printf("in tokens loop,%s, %s\n",tok,currFile);
	/* add word to result*/
	map_data[rank][count].word=tok;
	map_data[rank][count].docName=currFile;
	
	//printf("inserted word=> %s\n",map_data[rank][count].word);
	tok = strtok (NULL," .,;:!-");
	/*advance iterator*/
        count++;
	}
	if(ftell(fp2) > my_job[1])
	 break;
	
	}
	
	}
	}
	
	if(rank ==0){
	  num_per_reducer= num_procs/2;
	}

	#pragma omp barrier
}

void reduce(){
	int a,i,j,k;
	int my_rank= omp_get_thread_num();
      
	comb_wordkey *reduce_result;

	reduce_result=malloc((num_per_reducer*num_args*500)*sizeof(Word_key));
	output=malloc((2*(num_per_reducer*num_args*500))*sizeof(Word_key));
	
	/*allocate memory for reduce_result*/
	for(i=0;i<(num_per_reducer*num_args*(500));i++){
		reduce_result[i].docName=malloc((num_args-2)*sizeof(char));
		reduce_result[i].word=malloc(sizeof(char));
		reduce_result[i].word=NULL;
		for(j=0;j<(num_args-2);j++){
			reduce_result[i].docName[j]=malloc(sizeof(char));
			reduce_result[i].docName[j]=NULL;
		}
	}

	/*allocate memory for output list*/
        for(i=0;i<(num_per_reducer*num_args*(500));i++){
                output[i].docName=malloc((num_args-2)*sizeof(char));
                output[i].word=malloc(sizeof(char));
                output[i].word=NULL;
                for(j=0;j<(num_args-2);j++){
                        output[i].docName[j]=malloc(sizeof(char));
                        output[i].docName[j]=NULL;
                }
        }
	
	/* combine and merge 1st half of maps */
	if(my_rank == 0){
	for(i=0;i<num_per_reducer;i++){
		for(j=0;j<((num_args-2)*500);j++){
		if(map_data[i][j].word != NULL){
			for(k=0;k<(num_args*(500));k++){	
				if(output[k].word != NULL){

				if(output[k].word == map_data[i][j].word){
					for(a=0;a<num_args-2;a++){
					if(output[k].docName[a] != NULL){
						//printf("not null\n");
					if(output[k].docName[a] == map_data[i][j].docName){
						//printf("word+doc found\n");
						break;
					}
					}else{
						output[k].docName[a]=map_data[i][j].docName;
                                                //printf("added new docname\n");
						break;
					}
					}

				}

				}else{
					//printf("in new word\n");                       
					output[k].word= map_data[i][j].word;
                        		output[k].docName[0]= map_data[i][j].docName;
                        		break;


				}
			}
		}else break;
	}
	}
	}

	/* combine and merge second half of map*/
	if(my_rank ==1){
		//printf("rank 2");
	for(i=num_per_reducer;i<num_per_reducer*2;i++){
		for(j=0;j<((num_args-2)*500);j++){
                if(map_data[i][j].word != NULL){
                        for(k=0;k<(num_args*(500));k++){
                                if(reduce_result[k].word != NULL){

                                if(reduce_result[k].word == map_data[i][j].word){
                                        for(a=0;a<num_args-2;a++){
                                        if(reduce_result[k].docName[a] != NULL){
                                                //printf("not null\n");
                                        if(reduce_result[k].docName[a] == map_data[i][j].docName){
                                                //printf("word+doc found\n");
                                                break;
                                        }
                                        }else{
                                                reduce_result[k].docName[a]=map_data[i][j].docName;
                                                //printf("added new docname\n");
                                                break;
                                        }
                                        }

                                }

                                }else{
                                        //printf("in new word\n");
                                        reduce_result[k].word= map_data[i][j].word;
                                        reduce_result[k].docName[0]= map_data[i][j].docName;
                                        break;


                                }
                        }
                }else break;
        }
        }
	}

	#pragma omp barrier
	
	/* combine two reduce lists*/
	if(my_rank ==1){

         for(i=0;i<(num_per_reducer*num_args*500);i++){
                if(reduce_result[i].word != NULL){
                        for(k=0;k<(2*num_per_reducer*num_args*500);k++){
                                if(output[k].word != NULL){

                                if(output[k].word == reduce_result[i].word){
					int found[num_args-2];
                                        for(a=0;a<num_args-2;a++){
                                        if(output[k].docName[a] != NULL){
                                                //printf("not null\n");

					for(j=0;j<num_args-2;j++){
                                        if(output[k].docName[a] == reduce_result[i].docName[j]){
                                                //printf("word+doc found\n");
						found[j]=1;
                                                break;
                                        
                                        }

                                        }
                                        }
					}
						for(a=0;a<num_args-2;a++){
                                                   if(output[k].docName[a] == NULL){
							for(j=0;j<num_args-2;j++){
							if(found[j] ==0){
                                                	output[k].docName[a]=reduce_result[i].docName[j];
                                                	//printf("added new docname\n");
                                               		break;
							}
							}
						}
                                        }

				}
                                
				
                                }else{
                                       // printf("in new word\n");
                                        output[k].word= reduce_result[i].word;
                                        output[k].docName = reduce_result[i].docName;
                                        break;

				}
                                }
           		             
                }else break;
        }
        }		
	
	free(reduce_result);
	#pragma omp barrier
}


long usecs (void){
  struct timeval t;

  gettimeofday(&t,NULL);
  return t.tv_sec*1000000+t.tv_usec;
}


void write_output_to_file(){

	int j,i=0;
	FILE *fp;
	char *str= "\n";
	fp=fopen(outputFile,"w+");
        if(fp == NULL){
          printf("could not open file %s\n",outputFile);

        }else{
	/* build line message and write to file */
	while(output[i].word != NULL){

	fprintf(fp,"%s ",output[i].word);
       
	for(j=0;j<num_args-2;j++){
	if(output[i].docName[j] == NULL) break;
	fprintf(fp,"=>%s ",output[i].docName[j]);
	
	}
	fprintf(fp,"%s ",str);
	
	i++;
	}
	
	}
	fclose(fp);
}

int main(int argc, char *argv[]){
	long t_start, t_end;
	double time=0.0;
	int i,j,thread_count,count=2;
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
	
	/* allocate memory for per proces map result storage */	
	map_data = malloc(thread_count*sizeof(Word_key));
	for(i=0;i<thread_count;i++){
	map_data[i]= malloc(((num_args-2)*(500))*sizeof(Word_key));
		for(j=0;j<(num_args-2)*500;j++){
		map_data[i][j].word= malloc(sizeof(char));
		map_data[i][j].docName= malloc(sizeof(char));
		map_data[i][j].word=NULL;
		map_data[i][j].docName=NULL;
		}

	}

	t_start = usecs();
	#pragma omp parallel num_threads(thread_count)
	map();

	#pragma omp parallel num_threads(2)
	reduce();
	t_end = usecs();
	
	time =((double)(t_end - t_start))/1000000;	
	write_output_to_file();
	free(output);
	free(map_data);
	printf("Compute time for %d threads =>%lf\n",thread_count,time);
	return 0;
}

