
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

typedef enum { FIRST_FIT, WORST_FIT, BEST_FIT } alloc_t ;

typedef struct KV {
	int fd_h;
	int fd_kv;
	int fd_blk;
	int fd_dkv;
	alloc_t alloc;
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







KV *kv_open (const char *dbname, const char *mode, int hidx, alloc_t alloc){
	KV *new = malloc(sizeof(KV));
	new->alloc = alloc;
	char *extend = malloc(TAILLE_MIN);
	char *file = malloc(TAILLE_MIN);
	int fd1, fd2, fd3, fd4;

	if ( (strcmp(mode,"r"))==0 ) {
		extend = ".blk";
		snprintf(file,PATH_MAX,"%s/%s",dbname,extend);
		fd1 = open(file,O_RDONLY | O_CREAT);
		new->fd_blk = fd1;
		write(fd1,"1\n",1);

		extend = ".dkv";
		snprintf(file,PATH_MAX,"%s/%s",dbname,extend);
		fd2 = open(file,O_RDONLY | O_CREAT);
		new->fd_dkv = fd2;
		write(fd1,"2\n",1);

		extend = ".kv";
		snprintf(file,PATH_MAX,"%s/%s",dbname,extend);
		fd3 = open(file,O_RDONLY | O_CREAT);
		new->fd_kv= fd3;
		write(fd1,"3\n",1);

		extend = ".h";
		snprintf(file,PATH_MAX,"%s/%s",dbname,extend);
		fd4 = open(file,O_RDONLY | O_CREAT);
		new->fd_h = fd4;
		write(fd1,"4\n",1);

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
	if (val==NULL)
		val = malloc(sizeof(kv_datum));

		



	return 0;
}


int kv_put (KV *kv, const kv_datum *key, const kv_datum *val) {

	return 0;
}


int kv_del (KV *kv, const kv_datum *key) {

	return 0;
}


void kv_start (KV *kv) {

}


int kv_next (KV *kv, kv_datum *key, kv_datum *val) {
	return 0;
}

int main(int argc, char const *argv[]) {
	return 0;
}