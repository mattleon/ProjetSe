
//#include "kv.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <linux/limits.h>
#include <string.h>
#include <unistd.h>
#define TAILLE_MIN 32
#define TAILLE_MAX 2048
#define head_h 2
#define head_blk 1
#define head_kv 1
#define head_dkv 1
volatile int taille_num=0;


typedef enum { FIRST_FIT, WORST_FIT, BEST_FIT } alloc_t ;



typedef struct KV {
	int fd_h;
	int fd_kv;
	int fd_blk;
	int fd_dkv;
	alloc_t alloc;
	int hash;
} KV ;

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

/* Structure blk du .blk :
 * En-tête contient quatre infos :
 * le nombre d'entrée (0<=n<=1020)
 * Le numéro du prochain bloc
 * 0 si le prochain bloc n'existe pas, sinon 1
 * le numéro du bloc
 */
int write_blk(KV *kv, char * num) {
	int n;
	taille_num = sizeof(num);
	char *tmp = 0;
	if ( (n=write(kv->fd_blk,num,taille_num)==-1) ) { //écrite du num du bloc
		perror("Function write_blk");
		return 0;
	}
	if ( (n=write(kv->fd_blk,tmp,taille_num)==-1) ) {//écrite du nombre d'entrée
		perror("Write_blk");
		return 0;
	}
	if ( (n=write(kv->fd_blk,tmp,taille_num)==-1) ) { //Existence ou pas du prochain bloc
		perror("Write_blk");
		return 0;
	}
	if ( (n=write(kv->fd_blk,tmp,taille_num))==-1 ) { //écriture du num du prochain bloc
		perror("Write_blk");
		return 0;
	}

	return 1;
}

 len_t hash_0(kv_datum *a){
 	unsigned int sum=0;
 	unsigned int i;
 	for(i=0; i<a->len; i++)
 		sum+=((char*)a->ptr)[i];
 	printf("sum1 %d\n", sum%999983);
 	return sum%999983;
 }

 len_t hash_1(kv_datum *a){
 	unsigned int sum=0;
 	unsigned int i=0;
 	for(i=0; i<a->len; i++)
 		sum+=sum+((char*)a->ptr)[i];
 	printf("sum2 %d\n", sum%4096);
 	return sum%4096;
 }

 len_t hash_2(kv_datum *a){
 	unsigned int sum=0;
 	unsigned int i=0;
 	for(i=0; i<a->len; i++)
 		sum+=(sum+i+1)*((char*)a->ptr)[i];
 	printf("sum2 %d\n", sum%4096);
 	return sum%4096;
 }

 KV *kv_open (const char *dbname, const char *mode, int hidx, alloc_t alloc){
 	KV *new = malloc(sizeof(KV));
 	new->alloc = alloc;
	new->hash = hidx;
 	char *extend = malloc(TAILLE_MIN);
 	char *file = malloc(TAILLE_MIN);
 	int fd1, fd2, fd3, fd4;
 	struct stat bufferfile;


 	if ( (strcmp(mode,"r"))==0 ) {
 		extend = ".blk";
 		snprintf(file,PATH_MAX,"%s/%s",dbname,extend);
 		lstat(file, &bufferfile);
 		if(!S_ISREG(bufferfile.st_mode)){
 			fd1 = open(file, O_CREAT);
 			write(fd1,"1\n",1);
 			close(fd1);
 		}
 		fd1 = open(file, O_RDONLY);
 		new->fd_blk = fd1;


 		extend = ".dkv";
 		snprintf(file,PATH_MAX,"%s/%s",dbname,extend);
 		lstat(file, &bufferfile);
 		if(!S_ISREG(bufferfile.st_mode)){
 			fd2 = open(file, O_CREAT);
 			write(fd2,"2\n",1);
 			close(fd2);
 		}
 		fd2 = open(file, O_RDONLY);
 		new->fd_dkv = fd2;


 		extend = ".kv";
 		snprintf(file,PATH_MAX,"%s/%s",dbname,extend);
 		lstat(file, &bufferfile);
 		if(!S_ISREG(bufferfile.st_mode)){
 			fd3 = open(file, O_CREAT);
 			write(fd3,"3\n",1);
 			close(fd3);
 		}
 		fd3 = open(file, O_RDONLY);
 		new->fd_kv= fd3;


 		extend = ".h";
 		snprintf(file,PATH_MAX,"%s/%s",dbname,extend);
 		lstat(file, &bufferfile);
 		if(!S_ISREG(bufferfile.st_mode)){
 			fd4 = open(file, O_CREAT);
 			write(fd4,"4\n",1);
 			close(fd4);
 		}
 		fd4 = open(file,O_RDONLY);
 		new->fd_h = fd4;

 	}

 	if ( (strcmp(mode,"r+"))==0 ) {
 		extend = ".blk";
 		snprintf(file,PATH_MAX,"%s/%s",dbname,extend);
 		fd1 = open(file,O_RDWR | O_CREAT);
 		new->fd_blk = fd1;
 		write(fd1,"1\n",1);

 		extend = ".dkv";
 		snprintf(file,PATH_MAX,"%s/%s",dbname,extend);
 		fd2 = open(file,O_RDWR | O_CREAT);
 		new->fd_dkv = fd2;
 		write(fd1,"2\n",1);

 		extend = ".kv";
 		snprintf(file,PATH_MAX,"%s/%s",dbname,extend);
 		fd3 = open(file,O_RDWR | O_CREAT);
 		new->fd_kv= fd3;
 		write(fd1,"3\n",1);

 		extend = ".h";
 		snprintf(file,PATH_MAX,"%s/%s",dbname,extend);
 		fd4 = open(file,O_RDWR | O_CREAT);
 		new->fd_h = fd4;
 		write(fd1,"4\n",1);
 	}

 	if ( (strcmp(mode,"w"))==0 ) {
 		extend = ".blk";
 		snprintf(file,PATH_MAX,"%s/%s",dbname,extend);
 		fd1 = open(file,O_TRUNC | O_WRONLY | O_CREAT);
 		new->fd_blk = fd1;
 		write(fd1,"1\n",1);

 		extend = ".dkv";
 		snprintf(file,PATH_MAX,"%s/%s",dbname,extend);
 		fd2 = open(file,O_TRUNC | O_WRONLY | O_CREAT);
 		new->fd_dkv = fd2;
 		write(fd1,"2\n",1);

 		extend = ".kv";
 		snprintf(file,PATH_MAX,"%s/%s",dbname,extend);
 		fd3 = open(file,O_TRUNC | O_WRONLY | O_CREAT);
 		new->fd_kv= fd3;
 		write(fd1,"3\n",1);

 		extend = ".h";
 		snprintf(file,PATH_MAX,"%s/%s",dbname,extend);
 		fd4 = open(file,O_TRUNC | O_WRONLY | O_CREAT);
 		new->fd_h = fd4;
 		write(fd1,"4\n",1);
 	}


 	if ( (strcmp(mode,"w+"))==0 ) {
 		extend = ".blk";
 		snprintf(file,PATH_MAX,"%s/%s",dbname,extend);
 		fd1 = open(file,O_RDWR | O_TRUNC | O_CREAT);
 		new->fd_blk = fd1;
 		write(fd1,"1\n",1);

 		extend = ".dkv";
 		snprintf(file,PATH_MAX,"%s/%s",dbname,extend);
 		fd2 = open(file,O_RDWR | O_TRUNC | O_CREAT);
 		new->fd_dkv = fd2;
 		write(fd1,"2\n",1);

 		extend = ".kv";
 		snprintf(file,PATH_MAX,"%s/%s",dbname,extend);
 		fd3 = open(file,O_RDWR | O_TRUNC | O_CREAT);
 		new->fd_kv= fd3;
 		write(fd1,"3\n",1);

 		extend = ".h";
 		snprintf(file,PATH_MAX,"%s/%s",dbname,extend);
 		fd4 = open(file,O_RDWR | O_TRUNC | O_CREAT);
 		new->fd_h = fd4;
 		write(fd1,"4\n",1);
 	}

 	return new;
 }




int kv_close (KV *kv) {
	if ( close(kv->fd_h)==0 && close(kv->fd_kv)==0 && close(kv->fd_blk)==0 && close(kv->fd_dkv)==0 )
		return 0;
	else {
		perror("Close:");
		return -1;
	}
}



int kv_get (KV *kv, const kv_datum *key, kv_datum *val) {

	void *value;
	lseek(kv->fd_kv,head_kv,SEEK_SET);
	lseek(kv->fd_dkv,head_dkv,SEEK_SET);
	int offset, lg1;
	int validate = 0;
	int n,i;
	char *buf = malloc(TAILLE_MIN);

	int tmp;
	switch(kv->hash) {
		case 0: tmp = hash_0(key->ptr);
									break;

		case 1: tmp = hash_1(key->ptr);
									break;

		case 2: tmp = hash_2(key->ptr);
									break;

		default: return -1;
	}

	lseek(kv->fd_h,tmp,SEEK_SET);
	n=read(kv->fd_h,buf,sizeof(int));
	lseek(kv->fd_blk,atoi(buf),SEEK_SET);
	n=read(kv->fd_blk,buf,sizeof(int)); //numéro du prochain bloc (0 si pas de prochains bloc)
	if ( atoi(buf)!=0 ) {     /* A CHANGER POUR TOUS LES BLOCS !!! */
		n=read(kv->fd_blk,buf,sizeof(int)); // taille en nbr d'entrées du bloc
		for ( i = 0; i < atoi(buf); i++) { // 1022 int max!
			n=read(kv->fd_blk,buf,sizeof(int));
			lseek(kv->fd_kv,atoi(buf),SEEK_SET);
			n=read(kv->fd_blk,buf,sizeof(int));
			int tmp = atoi(buf);
			n=read(kv->fd_blk,buf,tmp);
			if (strcmp(buf,(char *)key->ptr)==0) {
				validate = 1;
				n=read(kv->fd_blk,buf,sizeof(int));
				int tmp = atoi(buf);
				n=read(kv->fd_blk,buf,tmp);
				value = (char *)buf;
			}
		}

	}

	if ( validate==1 ) {
		while( (n=read(kv->fd_dkv,buf,sizeof(int)))>0) {
			if ( strcmp(buf,"1")==0) {
				n=read(kv->fd_dkv,buf,sizeof(int));
				n=read(kv->fd_dkv,buf,8);
				offset = atoi(buf);
				lseek(kv->fd_kv,offset,SEEK_SET);
				n=read(kv->fd_dkv,buf,sizeof(int));
				lg1 = atoi(buf);
				n=read(kv->fd_dkv,buf,lg1);

				if (strcmp(buf,(char *)key->ptr)==0) {
					n=read(kv->fd_dkv,buf,sizeof(int));
					lg1 = atoi(buf);
					n=read(kv->fd_dkv,buf,lg1);
					if ( strcmp(value,buf)==0 ) {
						if (val==NULL) {
							val=malloc(sizeof(kv_datum));
							val->ptr = (void *)value;
							val->len = sizeof(val->ptr);
							free(buf);
							return 1;
						}
						else {
							val->ptr = (void *)buf;
							val->ptr = realloc(val->ptr,val->len);
							free(buf);
							return 1;
						}
					}
				}
				else {
					n=read(kv->fd_dkv,buf,sizeof(int) + 8);
				}
			}
		}
	}
	free(buf);
	return 0;
}

/*
int kv_put (KV *kv, const kv_datum *key, const kv_datum *val) {

	return 0;
}
*/

void kv_start (KV *kv) {
	lseek(kv->fd_blk,head_blk,SEEK_SET);
	lseek(kv->fd_h,head_h,SEEK_SET);
	lseek(kv->fd_kv,head_kv,SEEK_SET);
	lseek(kv->fd_dkv,head_dkv,SEEK_SET);
}


int kv_del (KV *kv, const kv_datum *key) {

	return 0;
}





int kv_next (KV *kv, kv_datum *key, kv_datum *val) {
	kv_start(kv);
	int n, lg;
	char *buf = malloc(TAILLE_MAX);
	if ( (n=read(kv->fd_kv,buf,sizeof(int)))==0  ) { //On la longueur de la clé dans le fichier .kv
		key=NULL;
		val=NULL;
		free(buf);
		return 0;
	}
	lg = atoi(buf);
	read(kv->fd_kv,buf,lg);
	if (key==NULL)
		key=malloc(sizeof(kv_datum));
	key->ptr = (void *)buf;
	key->len = lg;
	/* On pourrait maintenant faire un kv_get, mais il n'y a que deux lectures à
	faire, aussi on fera directement la lecture au lieu d'appeler la fonction */

	read(kv->fd_kv,buf,sizeof(int));
	lg = atoi(buf);
	read(kv->fd_kv,buf,lg);
	if (val==NULL)
		val=malloc(sizeof(kv_datum));
	val->ptr = (void *)buf;
	val->len = lg;
	free(buf);
	return 1;
}

int main() {

	return 0;
}
