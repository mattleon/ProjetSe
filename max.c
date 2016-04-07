
//#include "kv.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <time.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <linux/limits.h>
#include <string.h>
#include <unistd.h>
#define TAILLE_MIN 32
#define TAILLE_MAX 2048
#define head_h 2*sizeof(int)
#define head_blk sizeof(int)
#define head_kv sizeof(int)
#define head_dkv sizeof(int)


typedef enum { FIRST_FIT, WORST_FIT, BEST_FIT } alloc_t ;

typedef struct KV {
	int fd_h;
	int fd_kv;
	int fd_blk;
	int fd_dkv;
	alloc_t alloc;
	int hash;
} KV ;

typedef struct BLK {
	int numBlk;
	int nbrEnt; //Max 1020
	int numExBlk;
	int nextBlk;
} BLK;

/*
 * Avec des longueurs sur 32 bits, on peut aller jusqu'à 4 Go
 */

typedef uint32_t len_t ;

/*
 * Représente une donnée : soit une clef, soit une valeur
 */

struct kv_datum
{
    void  *ptr ;		/* la donnée elle-même */
    len_t len ;			/* sa taille */
} ;

typedef struct kv_datum kv_datum ;


/*
 * Les différents types d'allocation
 */




KV *kv_open (const char *dbname, const char *mode, int hidx, alloc_t alloc){
	KV *new = malloc(sizeof(KV));
	new->alloc = alloc;
	char *extend = malloc(TAILLE_MIN);
	char *file = malloc(TAILLE_MIN);
	int fd1, fd2, fd3, fd4;
	new->hash=hidx;
	struct stat bufferfile;
	char * buff=malloc(sizeof(int));
	if ( (strcmp(mode,"r"))==0 ) {

		extend = ".blk";
		snprintf(file, PATH_MAX, "%s%s", dbname, extend);
		lstat(file, &bufferfile);

		if(!S_ISREG(bufferfile.st_mode)){
			return NULL;
		}

		extend = ".blk";
		snprintf(file, PATH_MAX, "%s%s", dbname, extend);
		if((fd1 = open(file, O_RDONLY))==-1)
			return NULL;
		if((read(fd1, "1", sizeof(len_t)))==-1)
			return NULL;
		new->fd_blk = fd1;

		extend = ".dkv";
		snprintf(file, PATH_MAX, "%s%s", dbname, extend);
		if((fd2 = open(file, O_RDONLY))==-1)
			return NULL;
		if((read(fd2, "2", sizeof(len_t)))==-1)
			return NULL;
		new->fd_dkv = fd2;

		extend = ".kv";
		snprintf(file, PATH_MAX, "%s%s", dbname, extend);
		if((fd3 = open(file, O_RDONLY))==-1)
			return NULL;
		if((read(fd3, "3", sizeof(len_t)))==-1)
			return NULL;
		new->fd_kv= fd3;

		extend = ".h";
		snprintf(file, PATH_MAX, "%s%s", dbname, extend);
		if((fd4 = open(file, O_RDONLY))==-1)
			return NULL;
		if((read(fd4, "4", sizeof(len_t)))==-1)
			return NULL;
		new->fd_h = fd4;
	}

	else if ( (strcmp(mode,"r+"))==0 ) {

		extend = ".blk";
		snprintf(file, PATH_MAX, "%s%s", dbname, extend);
		lstat(file, &bufferfile);

		if(!S_ISREG(bufferfile.st_mode)){
			if((fd1 = open(file, O_CREAT | O_WRONLY, 0600))==-1)
				return NULL;
			if(write(fd1, "1", sizeof(len_t))==-1)
				return NULL;
			if(write(fd1, "0", sizeof(len_t))==-1)
				return NULL;
			if(close(fd1)==-1)
				return NULL;

			extend = ".dkv";
			snprintf(file, PATH_MAX, "%s%s", dbname, extend);
			if((fd2 = open(file, O_CREAT | O_WRONLY, 0600))==-1)
				return NULL;
			if(write(fd2, "2", sizeof(len_t))==-1)
				return NULL;
			if(write(fd2, "1", sizeof(len_t))==-1)
				return NULL;
			if(write(fd2, "0", 1)==-1)
				return NULL;
			if(write(fd2, "4294967296", sizeof(len_t))==-1)
				return NULL;
			if(write(fd2, "4", sizeof(len_t))==-1)
				return NULL;
			if(close(fd2)==-1)
				return NULL;

			extend = ".kv";
			snprintf(file, PATH_MAX, "%s%s", dbname, extend);
			if((fd3 = open(file, O_CREAT | O_WRONLY, 0600))==-1)
				return NULL;
			if(write(fd3, "3", sizeof(len_t))==-1)
				return NULL;
			if(close(fd3)==-1)
				return NULL;

			extend = ".h";
			snprintf(file, PATH_MAX, "%s%s", dbname, extend);
			if((fd4 = open(file, O_CREAT | O_WRONLY, 0600))==-1)
				return NULL;
			sprintf(buff, "%d", 4);
			if(write(fd4, buff, sizeof(len_t))==-1)
				return NULL;
			sprintf(buff, "%d", hidx);
			if(write(fd4, buff, sizeof(len_t))==-1)
				return NULL;
			if(close(fd4)==-1)
				return NULL;
		}

		extend = ".blk";
		snprintf(file, PATH_MAX, "%s%s", dbname, extend);
		if((fd1 = open(file, O_RDWR))==-1)
			return NULL;
		if(read(fd1, "1", sizeof(len_t))==-1)
			return NULL;
		new->fd_blk = fd1;

		extend = ".dkv";
		snprintf(file,PATH_MAX,"%s%s",dbname,extend);
		if((fd2 = open(file, O_RDWR))==-1)
			return NULL;
		if(read(fd2, "2", sizeof(len_t))==-1)
			return NULL;
		new->fd_blk = fd2;

		extend = ".kv";
		snprintf(file,PATH_MAX,"%s%s",dbname,extend);
		if((fd3 = open(file,O_RDWR))==-1)
			return NULL;
		if(read(fd3, "3", sizeof(len_t))==-1)
			return NULL;
		new->fd_kv= fd3;

		extend = ".h";
		snprintf(file,PATH_MAX,"%s%s",dbname,extend);
		if((fd4 = open(file,O_RDWR))==-1)
			return NULL;
		if(read(fd4, "4", sizeof(len_t))==-1)
			return NULL;
		new->fd_h = fd4;
	}

	else if ( (strcmp(mode,"w"))==0 ) {
	
		extend = ".blk";
		snprintf(file,PATH_MAX,"%s%s",dbname,extend);
		lstat(file, &bufferfile);

		if(!S_ISREG(bufferfile.st_mode)){
			return NULL;
		}

		extend = ".blk";
		snprintf(file, PATH_MAX, "%s%s", dbname, extend);
		if((fd1 = open(file, O_TRUNC | O_WRONLY))==-1)
			return NULL;
		if(read(fd1, "1", sizeof(len_t))==-1)
			return NULL;
		new->fd_blk = fd1;

		extend = ".dkv";
		snprintf(file, PATH_MAX, "%s%s", dbname, extend);
		if((fd2 = open(file, O_TRUNC | O_WRONLY))==-1)
			return NULL;
		if(read(fd2, "2", sizeof(len_t))==-1)
			return NULL;
		new->fd_dkv = fd2;

		extend = ".kv";
		snprintf(file,PATH_MAX,"%s%s",dbname,extend);
		if((fd3 = open(file, O_TRUNC | O_WRONLY))==-1)
			return NULL;
		if(read(fd3, "3", sizeof(len_t))==-1)
			return NULL;
		new->fd_kv= fd3;

		extend = ".h";
		snprintf(file, PATH_MAX, "%s%s", dbname, extend);
		if((fd4 = open(file, O_TRUNC | O_WRONLY))==-1)
			return NULL;
		if(read(fd4, "4", sizeof(len_t))==-1)
			return NULL;
		new->fd_h = fd4;
	}


	else if ( (strcmp(mode,"w+"))==0 ) {

		extend = ".blk";
		snprintf(file, PATH_MAX, "%s%s", dbname, extend);
		lstat(file, &bufferfile);

		if(!S_ISREG(bufferfile.st_mode)){
			if((fd1 = open(file, O_CREAT | O_WRONLY, 0600))==-1)
				return NULL;
			if(write(fd1, "1", sizeof(len_t))==-1)
				return NULL;
			if(write(fd1, "0", sizeof(len_t))==-1)
				return NULL;
			if(close(fd1)==-1)
				return NULL;

			extend = ".dkv";
			snprintf(file,PATH_MAX,"%s%s",dbname,extend);
			if((fd2 = open(file, O_CREAT | O_WRONLY, 0600))==-1)
				return NULL;
			if(write(fd2, "2", sizeof(len_t))==-1)
				return NULL;
			if(write(fd2, "1", sizeof(len_t))==-1)
				return NULL;
			if(write(fd2, "0", 1)==-1)
				return NULL;
			if(write(fd2, "4294967296", sizeof(len_t))==-1)
				return NULL;
			if(write(fd2, "4", sizeof(len_t))==-1)
				return NULL;
			if(close(fd2)==-1)
				return NULL;

			extend = ".kv";
			snprintf(file, PATH_MAX, "%s%s", dbname, extend);
			if((fd3 = open(file, O_CREAT | O_WRONLY, 0600))==-1)
				return NULL;
			if(write(fd3, "3", sizeof(len_t))==-1)
				return NULL;
			if(close(fd3)==-1)
				return NULL;

			extend = ".h";
			snprintf(file, PATH_MAX, "%s%s", dbname, extend);
			if((fd4 = open(file, O_CREAT | O_WRONLY, 0600))==-1)
				return NULL;
			sprintf(buff, "%d", 4);
			if(write(fd4, buff, sizeof(len_t))==-1)
				return NULL;
			sprintf(buff, "%d", hidx);
			if(write(fd4, buff, sizeof(len_t))==-1)
				return NULL;
			if(close(fd4)==-1)
				return NULL;
		}

		extend = ".blk";
		snprintf(file, PATH_MAX, "%s%s", dbname, extend);
		if((fd1 = open(file, O_TRUNC | O_RDWR))==-1)
			return NULL;
		if(write(fd1, "1", sizeof(len_t))==-1)
			return NULL;
		if(write(fd1, "0", sizeof(len_t))==-1)
			return NULL;
		if(read(fd1, "1", sizeof(len_t))==-1)
			return NULL;
		new->fd_blk = fd1;

		extend = ".dkv";
		snprintf(file, PATH_MAX, "%s%s", dbname, extend);
		if((fd2 = open(file, O_TRUNC | O_RDWR))==-1)
			return NULL;
		if(write(fd2, "2", sizeof(len_t))==-1)
			return NULL;
		if(write(fd2, "1", sizeof(len_t))==-1)
			return NULL;
		if(write(fd2, "0", 1)==-1)
			return NULL;
		if(write(fd2, "4294967296", sizeof(len_t))==-1)
			return NULL;
		if(write(fd2, "4", sizeof(len_t))==-1)
			return NULL;
		if(read(fd2, "2", sizeof(len_t))==-1)
			return NULL;
		new->fd_dkv = fd2;

		extend = ".kv";
		snprintf(file, PATH_MAX, "%s%s", dbname, extend);
		if((fd3 = open(file, O_TRUNC | O_RDWR))==-1)
			return NULL;
		if(write(fd3, "3", sizeof(len_t))==-1)
			return NULL;
		if(read(fd3, "3", sizeof(len_t))==-1)
			return NULL;
		new->fd_kv= fd3;

		extend = ".h";
		snprintf(file, PATH_MAX, "%s%s", dbname, extend);
		if((fd4 = open(file, O_TRUNC | O_RDWR))==-1)
			return NULL;
		sprintf(buff, "%d", 4);
		if(write(fd4, buff, sizeof(len_t))==-1)
			return NULL;
		sprintf(buff, "%d", hidx);
		if(write(fd4, buff, sizeof(len_t))==-1)
			return NULL;
		if(read(fd4, "4", sizeof(len_t))==-1)
			return NULL;
		new->fd_h = fd4;
	}
	free(buff);
	free(file);
	return new;
}




int kv_close (KV *kv) {
	if ( close(kv->fd_h)==0 && close(kv->fd_kv)==0 && close(kv->fd_blk)==0 && close(kv->fd_dkv)==0 )
		return 0;
	else {
		return -1;
	}
}



int kv_get (KV *kv, const kv_datum *key, kv_datum *val) {
	if (val==NULL)
		val = malloc(sizeof(kv_datum));

	lseek(kv->fd_kv,head_kv,SEEK_SET);
	lseek(kv->fd_dkv,head_dkv,SEEK_SET);
	int offset, lg1;

	int n;
	char *buf = malloc(TAILLE_MIN);

	while( (n=read(kv->fd_dkv,buf,sizeof(int)))>0) {
		if ( strcmp(buf,"1")==0) {
			n=read(kv->fd_dkv,buf,sizeof(int));
			n=read(kv->fd_dkv,buf,8);
			offset = atoi(buf);
			lseek(kv->fd_kv,offset,SEEK_SET);
			n=read(kv->fd_dkv,buf,sizeof(int));
			lg1 = atoi(buf);
			n=read(kv->fd_dkv,buf,lg1);

			if (strcmp(buf,key->ptr)==0) {
				n=read(kv->fd_dkv,buf,sizeof(int));
				lg1 = atoi(buf);
				n=read(kv->fd_dkv,buf,lg1);
				val->ptr = buf;
				return 1;
			}
			else {
				n=read(kv->fd_dkv,buf,sizeof(int) + 8);
			}
		}
	}

	return 0;
}

/*
int kv_put (KV *kv, const kv_datum *key, const kv_datum *val) {

	return 0;
}
*/

void kv_start (KV *kv) {
	if((lseek(kv->fd_blk,head_blk,SEEK_SET))==-1){
		perror("lseek fd_blk");
		exit(1);
	}
	if((lseek(kv->fd_h,head_h,SEEK_SET))==-1){
		perror("lseek fd_h");
		exit(1);
	}
	if((lseek(kv->fd_kv,head_kv,SEEK_SET))==-1){
		perror("lseek fd_kv");
		exit(1);
	}
	if((lseek(kv->fd_dkv,head_dkv,SEEK_SET))==-1){
		perror("lseek fd_dkv");
		exit(1);
	}
}

/*
int kv_del (KV *kv, const kv_datum *key) {
	
	return 0;
}*/




/*
int kv_next (KV *kv, kv_datum *key, kv_datum *val) {
	return 0;
}
*/
len_t fcth1(kv_datum *a){
	unsigned int sum=0;
	unsigned int i;
	for(i=0; i<a->len; i++)
		sum+=((char*)a->ptr)[i];
	printf("sum1 %d\n", sum%999983);
	return sum%999983;
}

len_t fcth2(kv_datum *a){
	unsigned int sum=0;
	unsigned int i=0;
	for(i=0; i<a->len; i++)
		sum+=sum+((char*)a->ptr)[i];
	printf("sum2 %d\n", sum%4096);
	return sum%4096;	
}

len_t fcth3(kv_datum *a){
	unsigned int sum=0;
	unsigned int i=0;
	for(i=0; i<a->len; i++)
		sum+=(sum+i+1)*((char*)a->ptr)[i];
	printf("sum2 %d\n", sum%4096);
	return sum%4096;	
} 

int main() {
	/*kv_datum *a=malloc(sizeof(kv_datum));
	a->ptr="azertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiopazertyuiop";
	a->len=1000;

    float temps;
    clock_t t1, t2;
 
    t1 = clock();

    fcth1(a); 
    t2 = clock();
    temps = (float)(t2-t1)/CLOCKS_PER_SEC;
    printf("temps = %f\n", temps);

    float temps2;
    clock_t tt1, tt2;
 
    tt1 = clock();
 
	fcth2(a);
     
    tt2 = clock();
    temps2 = (float)(tt2-tt1)/CLOCKS_PER_SEC;
    printf("temps2 = %f\n", temps2);

    float temps3;
    clock_t ttt1, ttt2;
 
    ttt1 = clock();
 
	fcth3(a);
     
    ttt2 = clock();
    temps3 = (float)(ttt2-ttt1)/CLOCKS_PER_SEC;
    printf("temps3 = %f\n", temps3);*/
	kv_open ("yolo", "w+", 2, 5);


	return 0;
}