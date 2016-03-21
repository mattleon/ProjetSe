#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include "kv.h"
#include <fcntl.h>
#include <sys/stat.h>
#define TAILLE_MAX 500
#include <linux/limits.h>

KV *kv_open (const char *dbname, const char *mode, int hidx, alloc_t alloc){
	char* file=malloc(TAILLE_MAX);
	KV *kv=malloc(sizeof(KV));
	if(strcmp(mode,"r")){
		snprintf(file, PATH_MAX, "%s/%s", dbname, ".h");
		kv->fdh=open(file, O_RDONLY | O_CREAT, 0666);
		if(kv->fdh==-1){
			perror("fichier .h");
			exit(1);
		}

		snprintf(file, PATH_MAX, "%s/%s", dbname, ".blk");
		kv->fdblk=open(file, O_RDONLY | O_CREAT, 0666);
		if(kv->fdblk==-1){
			perror("fichier .blk");
			exit(1);
		}

		snprintf(file, PATH_MAX, "%s/%s", dbname, ".kv");
		kv->fdkv=open(file, O_RDONLY | O_CREAT, 0666);
		if(kv->fdkv==-1){
			perror("fichier .kv");
			exit(1);
		}

		snprintf(file, PATH_MAX, "%s/%s", dbname, ".dkv");
		kv->fddkv=open(file, O_RDONLY | O_CREAT, 0666);
		if(kv->fddkv==-1){
			perror("fichier .dkv");
			exit(1);
		}
	}

	if(strcmp(mode,"r+")){
		snprintf(file, PATH_MAX, "%s/%s", dbname, ".h");
		kv->fdh=open(file, O_RDWR | O_CREAT, 0666);
		if(kv->fdh==-1){
			perror("fichier .h");
			exit(1);
		}

		snprintf(file, PATH_MAX, "%s/%s", dbname, ".blk");
		kv->fdblk=open(file, O_RDWR | O_CREAT, 0666);
		if(kv->fdblk==-1){
			perror("fichier .blk");
			exit(1);
		}

		snprintf(file, PATH_MAX, "%s/%s", dbname, ".kv");
		kv->fdkv=open(file, O_RDWR | O_CREAT, 0666);
		if(kv->fdkv==-1){
			perror("fichier .kv");
			exit(1);
		}

		snprintf(file, PATH_MAX, "%s/%s", dbname, ".dkv");
		kv->fddkv=open(file, O_RDWR | O_CREAT, 0666);
		if(kv->fddkv==-1){
			perror("fichier .dkv");
			exit(1);
		}
	}
	if(strcmp(mode,"w")){
		snprintf(file, PATH_MAX, "%s/%s", dbname, ".h");
		kv->fdh=open(file, O_WRONLY|O_TRUNC | O_CREAT, 0666);
		if(kv->fdh==-1){
			perror("fichier .h");
			exit(1);
		}

		snprintf(file, PATH_MAX, "%s/%s", dbname, ".blk");
		kv->fdblk=open(file, O_WRONLY|O_TRUNC| O_CREAT, 0666);
		if(kv->fdblk==-1){
			perror("fichier .blk");
			exit(1);
		}

		snprintf(file, PATH_MAX, "%s/%s", dbname, ".kv");
		kv->fdkv=open(file, O_WRONLY|O_TRUNC | O_CREAT, 0666);
		if(kv->fdkv==-1){
			perror("fichier .kv");
			exit(1);
		}

		snprintf(file, PATH_MAX, "%s/%s", dbname, ".dkv");
		kv->fddkv=open(file, O_WRONLY|O_TRUNC | O_CREAT, 0666);
		if(kv->fddkv==-1){
			perror("fichier .dkv");
			exit(1);
		}
	}
	if(strcmp(mode,"w+")){
		snprintf(file, PATH_MAX, "%s/%s", dbname, ".h");
		kv->fdh=open(file, O_RDWR|O_TRUNC | O_CREAT, 0666);
		if(kv->fdh==-1){
			perror("fichier .h");
			exit(1);
		}

		snprintf(file, PATH_MAX, "%s/%s", dbname, ".blk");
		kv->fdblk=open(file, O_RDWR|O_TRUNC | O_CREAT, 0666);
		if(kv->fdblk==-1){
			perror("fichier .blk");
			exit(1);
		}

		snprintf(file, PATH_MAX, "%s/%s", dbname, ".kv");
		kv->fdkv=open(file, O_RDWR|O_TRUNC | O_CREAT, 0666);
		if(kv->fdkv==-1){
			perror("fichier .kv");
			exit(1);
		}

		snprintf(file, PATH_MAX, "%s/%s", dbname, ".dkv");
		kv->fddkv=open(file, O_RDWR|O_TRUNC | O_CREAT, 0666);
		if(kv->fddkv==-1){
			perror("fichier .dkv");
			exit(1);
		}
	}
	char *hid=(char *)hidx;
	write(kv->fdh, "h", 4);
	write(kv->fdh, hid, 4);
	write(kv->fdblk, "blk", 4);
	write(kv->fdkv, "kv", 4);
	write(kv->fddkv, "dkv", 4);
	kv->alloc=alloc;
	free(file);
	free(hid);
	return kv;	
}

int kv_close (KV *kv){
	int n;
	if((n=(close(kv->fdh) && close(kv->fdblk) && close(kv->fdkv) && close(kv->fddkv)))==-1){
		perror("close");
		exit(1);
	}
	return 0;
}

/*int kv_get (KV *kv, const kv_datum *key, kv_datum *val){
	
}

int kv_put (KV *kv, const kv_datum *key, const kv_datum *val){

}

int kv_del (KV *kv, const kv_datum *key){

}

void kv_start (KV *kv){

}

int kv_next (KV *kv, kv_datum *key, kv_datum *val){

}*/