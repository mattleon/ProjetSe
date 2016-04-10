#include <time.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "kv.h"

len_t hash_0(const kv_datum *a, int fd){
 	unsigned int sum=0;
 	unsigned int i;
    char *str = malloc(a->len);
 	for(i=0; i<a->len; i++){
		read(fd, str, 1);
 		sum+=str[i];}
	printf("Somme 1ère fonction modulo 999983 : %d\n", sum%999983);
 	return sum%999983;
}

len_t hash_1(const kv_datum *a, int fd){
 	unsigned int sum=0;
 	unsigned int i=0;
    char *str = malloc(a->len);
 	for(i=0; i<a->len; i++){
		read(fd, str, 1);
 		sum+=2*(str[i]+1);}
	printf("Somme 2ère fonction modulo 999983 : %d\n", sum%999983);
 	return sum%999983;
}

len_t hash_2(const kv_datum *a, int fd){
 	unsigned int sum=0;
 	unsigned int i=0;
    char *str = malloc(a->len);
 	for(i=0; i<a->len; i++){
		read(fd, str, 1);
 		sum=(sum+i+1)*(str[i]+1);}
	printf("Somme 3ère fonction modulo 999983 : %d\n", sum%999983);
 	return sum%999983;
}

int main(int argc, char* argv[]){
	if(argc<2)
		return -1;
	kv_datum *a=malloc(sizeof(kv_datum));
	int fd=open(argv[1], O_RDONLY);
	int length=lseek(fd, 0, SEEK_END)+1;
	a->len=length;
	lseek(fd, 0, SEEK_SET);
	printf("Longueur fichier : %d \n", length);
   
	float temps;
    clock_t t1, t2;
 
    t1 = clock();

    hash_0(a, fd); 
    t2 = clock();
    temps = (float)(t2-t1)/CLOCKS_PER_SEC;
    printf("Temps 1ère fonction = %f\n", temps);


	lseek(fd, 0, SEEK_SET);
    float temps2;
    clock_t tt1, tt2;
 
    tt1 = clock();
 
	hash_1(a, fd);
     
    tt2 = clock();
    temps2 = (float)(tt2-tt1)/CLOCKS_PER_SEC;
    printf("Temps 2ème fonction = %f\n", temps2);


	lseek(fd, 0, SEEK_SET);
    float temps3;
    clock_t ttt1, ttt2;
 
    ttt1 = clock();
 
	hash_2(a, fd);
     
    ttt2 = clock();
    temps3 = (float)(ttt2-ttt1)/CLOCKS_PER_SEC;
    printf("Temps 3ème fonction = %f\n", temps3);	

	free(a);
	return 0;
}
