
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
#include <errno.h>
#define TAILLE_MIN 1024
#define TAILLE_MAX 4096
#define head_h 2
#define head_blk 1
#define head_kv 1
#define head_dkv 1
//volatile int taille_num=0;


typedef enum { FIRST_FIT, WORST_FIT, BEST_FIT } alloc_t ;

typedef struct BLK {
	int numBlk;
	int nbrEnt; //Max 1020
	int numExBlk;
	int nextBlk;
} BLK;



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

//typedef enum { FIRST_FIT, WORST_FIT, BEST_FIT } alloc_t ;

/* prend en argument l'offset correspondant à l'offset dans .blk */
len_t first_fit(KV *kv, kv_datum *key, kv_datum *val, len_t offset) {

	unsigned int n,lg;
	len_t off,off1;;
	char *buffer = malloc(TAILLE_MAX);
	char *buf = malloc(sizeof(len_t));
	lseek(kv->fd_dkv,head_dkv,SEEK_SET);
	while ( (n=read(kv->fd_dkv,buffer,1))!=0 ) {
		if (atoi(buffer)==0) {//Si emplacement libre
			n=read(kv->fd_dkv,buffer,sizeof(int));//Lecture longueur cellule .kv
			lg = atoi(buf);
			/* Vérification longueur suffisante
			 * suffisant si : place pour deux couples
			 * l'un qui est donné et l'autre qui contient une clé d'un octet
			 * et une valeur de 0 octets
			 */
			if ( lg>=(4*sizeof(len_t)+1+key->len + val->len)) {
				n=read(kv->fd_dkv,buffer,sizeof(int));//Récupération offset de la cellule dans .kv
				off = atoi(buffer);//Offset du début de la cellule dans .kv
				lseek(kv->fd_kv,off,SEEK_SET);
				sprintf(buf,"%d",key->len);
				write(kv->fd_dkv,buf,sizeof(len_t));//Ecriture de la longueur de la clé
				write(kv->fd_dkv,key->ptr,key->len);//Ecrite de la clé
				sprintf(buf,"%d",val->len);
				write(kv->fd_dkv,buf,sizeof(len_t));//ecriture de la lg de la val
				write(kv->fd_dkv,val->ptr,val->len);//Ecriture de la val
				off1 = lseek(kv->fd_kv,0,SEEK_SET);//On récupère l'offset actuel dans le .kv
				/* On se place dans le .blk pour y inscrire la valeur
				 * de l'offset dans le kv à l'offset donné en argument.
				 */
				lseek(kv->fd_blk,offset,SEEK_SET);
				sprintf(buf,"%d",off);
				write(kv->fd_blk,buf,sizeof(len_t));

			}
		}

	}



	return 0;
}
/*
len_t worst_fit(KV *kv, kv_datum *key, kv_datum *val, len_t offset) {



	return 0;
}

len_t best_fit(KV *kv, kv_datum *key, kv_datum *val, len_t offset) {



	return 0;
}
*/


int decale_haut(KV *kv, len_t offset) {
	char *buffer = malloc(TAILLE_MAX);
	int n;
	lseek(kv->fd_dkv,sizeof(len_t),SEEK_SET);
	n=read(kv->fd_dkv,buffer,sizeof(len_t));//On récupère le nombre de cellule
	lseek(kv->fd_dkv,sizeof(int),SEEK_SET);//On se place au nombre de cellules
	unsigned int nb = atoi(buffer)-1;//Nb cellule -1
	sprintf(buffer,"%d",nb);
	write(kv->fd_dkv,buffer,sizeof(len_t));
	lseek(kv->fd_dkv,offset,SEEK_SET);//On se place à la fin de la cellule à supprimer

	while( (n=read(kv->fd_dkv,buffer,1+2*sizeof(len_t))!=0) ) {//Tant qu'on lit
		lseek(kv->fd_dkv,-1-2*sizeof(len_t),SEEK_CUR);//On se place à la cellule précédente
		write(kv->fd_dkv,buffer,1+2*sizeof(len_t));//On écrit la cellule suivante lue
		lseek(kv->fd_dkv,1+2*sizeof(len_t),SEEK_CUR);//On se place à la cellule d'après
	}

	free(buffer);
	return 0;
}
/*A la fin de cette fonction, la dernière cellule est en double,
C'est pour ça qu'on met le nombre de cellules dans l'en-tête.*/


//L'offset correspond au début de la celulle à descendre
//A l'offset donné, on écrira la nouvelle cellule
int decale_bas(KV *kv, len_t offset) {
	char *buffer = malloc(TAILLE_MAX);
	unsigned int n, nb_cell,nb;
	unsigned int off,i;
	lseek(kv->fd_dkv,sizeof(len_t),SEEK_SET);
	n=read(kv->fd_dkv,buffer,sizeof(len_t));//On récupère le nombre de cellule
	lseek(kv->fd_dkv,sizeof(int),SEEK_SET);//On se place au nombre de cellules
	nb_cell  = atoi(buffer)+1;//Nb cellule +1
	sprintf(buffer,"%d",nb_cell);
	write(kv->fd_dkv,buffer,sizeof(len_t));//Ré-écriture de la nouvelle valeur
	//Le pointeur est donc à la fin de l'entête, à la première cellule;

	for(i=0;i<nb_cell-1;i++) {
		lseek(kv->fd_dkv,1+2*sizeof(len_t),SEEK_CUR);//On bouge d'une cellule
		off=lseek(kv->fd_dkv,0,SEEK_CUR);//Récupération de l'offset actel
		nb++;
		if (off==offset) {
			break;
		}
	}
	//A la fin de la boucle, on est donc au début de la cellule à déplacer
	//On a le nombre de cellules parcourues
	nb = nb_cell-nb; //Nombre de cellules restantes
	n=read(kv->fd_dkv,buffer,nb*(1+2*sizeof(len_t)));//On récupère toutes les cellules restantes
	lseek(kv->fd_dkv,offset+(1+2*sizeof(len_t)),SEEK_SET);
	write(kv->fd_dkv,buffer,nb*(1+2*sizeof(len_t)));



	return 0;
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

	BLK *blk = malloc(sizeof(BLK));
	lseek(kv->fd_kv,head_kv,SEEK_SET);
	lseek(kv->fd_dkv,head_dkv,SEEK_SET);
	int i,n,lg;
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

	lseek(kv->fd_h,head_h+tmp*sizeof(int),SEEK_SET);
	n=read(kv->fd_h,buf,sizeof(int));
	lseek(kv->fd_blk,atoi(buf),SEEK_SET);
	n=read(kv->fd_blk,buf,sizeof(int)); //Lecture num bloc
	blk->numBlk = atoi(buf);
	n=read(kv->fd_blk,buf,sizeof(int));//Lecture nombre entrée dans le bloc
	blk->nbrEnt = atoi(buf);
	n=read(kv->fd_blk,buf,sizeof(int));//Lecture existence bloc suivant
	blk->numExBlk = atoi(buf);
	n=read(kv->fd_blk,buf,sizeof(int));//Num du prochain bloc si existe (sinon 0)
	blk->nextBlk = atoi(buf);
	/* Le pointeur est maintenant situé à la première entrée du bloc,
	après l'en-tête */

	while(blk->numExBlk!=0) {
		for(i=0;i<blk->nbrEnt;i++) {
			n=read(kv->fd_blk,buf,sizeof(len_t));//Lecture de l'offset dans .blk
			if (n==0) {
				return 0;
			}
			lseek(kv->fd_kv,atoi(buf),SEEK_SET);//dans le .kv à offset
			n=read(kv->fd_dkv,buf,sizeof(int));//lit la longueur de la clé
			lg = atoi(buf);
			n=read(kv->fd_dkv,buf,lg);//lecture de la clé
			if ( strcmp(buf,(char *)key->ptr)==0 ) {
				if (val==NULL) {
					n=read(kv->fd_dkv,buf,sizeof(int));//Lecture de la longueur de val
					lg = atoi(buf);
					n=read(kv->fd_dkv,buf,lg);
					val=malloc(sizeof(kv_datum));
					val->ptr = (void *)buf;
					val->len = lg;
					free(buf);
					return 1;
				}
				else {
					lseek(kv->fd_kv,sizeof(int),SEEK_CUR);
					n=read(kv->fd_dkv,buf,val->len);
					val->ptr = (void *)buf;
					free(buf);
					return 1;
				}
			}
		}
		/* On se place au début du bloc suivant */
		if (blk->numExBlk!=0) {//Si le bloc suivant existe
			lseek(kv->fd_blk,head_blk + blk->nextBlk*4096,SEEK_SET);
			n=read(kv->fd_blk,buf,sizeof(int)); //Lecture num bloc
			blk->numBlk = atoi(buf);
			n=read(kv->fd_blk,buf,sizeof(int));//Lecture nombre entrée dans le bloc
			blk->nbrEnt = atoi(buf);
			n=read(kv->fd_blk,buf,sizeof(int));//Lecture existence bloc suivant
			blk->numExBlk = atoi(buf);
			n=read(kv->fd_blk,buf,sizeof(int));//Num du prochain bloc si existe (sinon 0)
			blk->nextBlk = atoi(buf);
		}
	}
	return 0;
}

/* Rentre le couple clé/valeur dans la base, remplace l'ancienne
 * valeur par la nouvelle si la clé est présente
 * Renvoie 1 en cas de succès, 0 sinon.
 */
int kv_put (KV *kv, const kv_datum *key, const kv_datum *val) {

	int tmp,n,offset,i,lg,pos;
	char *buffer = malloc(sizeof(len_t));
	char *bufNum = malloc(TAILLE_MAX);
	BLK *blk = malloc(sizeof(BLK));
	char *null = "0000";
	switch (kv->hash) {
		case 0: tmp = hash_0(key->ptr);
						break;

		case 1:tmp = hash_1(key->ptr);
						break;

		case 2:tmp = hash_2(key->ptr);
						break;

		default : break;
	}

	lseek(kv->fd_h,head_h+tmp*sizeof(len_t),SEEK_SET);
	n=read(kv->fd_h,buffer,sizeof(len_t));
	if (n==0) {
		/* Création d'une entrée dans le .h
		 * Création d'un bloc dans le .blk
		 * utilisation d'une fonction d'allocation
		 */






	}
	else {
		offset = atoi(buffer);
		lseek(kv->fd_blk,offset,SEEK_SET);
		n=read(kv->fd_blk,bufNum,sizeof(int)); //On lit le num du bloc
		blk->numBlk = atoi(bufNum);
		n=read(kv->fd_blk,bufNum,sizeof(int)); //nbr d'entrée
		blk->nbrEnt = atoi(bufNum);
		n=read(kv->fd_blk,bufNum,sizeof(int)); //existence bloc suivant
		blk->numExBlk = atoi(bufNum);
		n=read(kv->fd_blk,bufNum,sizeof(int)); //num bloc suivant (0 par défaut)
		blk->nextBlk = atoi(bufNum);

		while (blk->numExBlk!=0) {//Parcours de tous les blocs correspondant à la clé
			for(i=0;i<blk->nbrEnt;i++) {//Parcours de tous les offsets du bloc
				n=read(kv->fd_blk,bufNum,sizeof(len_t));//Lecture de l'offset
				if (strcmp(bufNum,null))
					pos = lseek(kv->fd_blk,0,SEEK_CUR);

				lseek(kv->fd_kv,atoi(bufNum),SEEK_SET);//On se place à l'offset dans .kv
				n=read(kv->fd_blk,bufNum,sizeof(int));//On lit la longueur de la clé
				lg = atoi(bufNum);
				n=read(kv->fd_blk,bufNum,lg);//On lit la clé
				if ( strcmp(bufNum,(char *)key->ptr)==0 ) {//Si les deux clés sont identiques
					n=read(kv->fd_blk,bufNum,sizeof(int));//On lit la longeur de la valeur
					lg = atoi(bufNum);
					write(kv->fd_kv,(char *)val->ptr,val->len);//On remplace la valeur
					return 1;//En cas de réussite
				}
			}

			lseek(kv->fd_blk,head_blk + blk->nextBlk*4096,SEEK_SET);
			n=read(kv->fd_blk,bufNum,sizeof(int)); //On lit le num du bloc
			blk->numBlk = atoi(bufNum);
			n=read(kv->fd_blk,bufNum,sizeof(int)); //nbr d'entrée
			blk->nbrEnt = atoi(bufNum);
			n=read(kv->fd_blk,bufNum,sizeof(int)); //existence bloc suivant
			blk->numExBlk = atoi(bufNum);
			n=read(kv->fd_blk,bufNum,sizeof(int)); //num bloc suivant (0 par défaut)
			blk->nextBlk = atoi(bufNum);
		}
		/* Si on est ici, alors la clé n'est pas encore dans la base
		 * mais sa valeur de hachage existe déjà, pas besoin de créer
		 * un bloc dans ce cas.
		 * On va alors se placer au premier offset nul trouvé dans les blocs,
		 * correspondant à l'offset pos.
		 */

		 /* Pos contient la position du curseur situé juste après le premier offset
		  * nul trouvé pendant la vérification des clés.
			*/
		 lseek(kv->fd_blk,pos-sizeof(len_t),SEEK_SET);
		 /* Il faut maintenant récupérer un offset libre dans le fichier .kv
		 en passant par le fichier .dkv.
		 utiliser les fonctions d'allocations worst/best.first fit.
		 */




	}




	return 0;
}


/* Cette fonction place le pointeur dans les fichiers .kv et .dkv juste après
 * l'en-tête
 */
void kv_start (KV *kv) {
	lseek(kv->fd_kv,head_kv,SEEK_SET);
	lseek(kv->fd_dkv,head_dkv,SEEK_SET);
}


/* On suppose que le pointeur est situé juste avant le 0
 * dans le descripteur que l'on vient de libérer
 * On suppose aussi que la fonction fusionne étant appelée à chaque del,
 * Il n'y a que deux cellules à fusionner, soit celle-là et celle du dessus,
 * soit celle-là et celle du dessous.
 */

void fusionne_cel(KV *kv) {
	int i,n,lg,offset;
	char *buffer = malloc(sizeof(len_t));
	lseek(kv->fd_dkv,-1-2*sizeof(len_t),SEEK_CUR);
	n=read(kv->fd_dkv,buffer,1);
	if ( atoi(buffer)==0) {

	}
}


/* Fonction intermédiaire qui s'occupe de vérifier si une clé est dans un bloc
 * Si la clé est dans le bloc, elle l'efface et renvoie 1, sinon renvoie 0
 * blk contient l'en-tête du bloc
 */
int del(KV *kv, BLK *blk,const kv_datum *key) {
	int n,i;
	char *bufNum = malloc(sizeof(len_t));
	char *buffer = malloc(TAILLE_MAX);

	lseek(kv->fd_blk,head_blk+blk->numExBlk*4096,SEEK_SET);
	n=read(kv->fd_blk,bufNum,sizeof(len_t)); //On lit le num du bloc
	blk->numBlk = atoi(bufNum);
	n=read(kv->fd_blk,bufNum,sizeof(len_t)); //nbr d'entrée
	blk->nbrEnt = atoi(bufNum);
	n=read(kv->fd_blk,bufNum,sizeof(len_t)); //existence bloc suivant
	blk->numExBlk = atoi(bufNum);
	n=read(kv->fd_blk,bufNum,sizeof(len_t)); //num bloc suivant (0 par défaut)
	blk->nextBlk = atoi(bufNum);

	for(i=0;i<blk->nbrEnt;i++) {
		n=read(kv->fd_h,bufNum,sizeof(len_t));
		if (strcmp(bufNum, (char *)key->ptr)==0) {
			while ( (n=read(kv->fd_dkv,buffer,1)>0) ) {
				if (strcmp(buffer,"1")) {
					read(kv->fd_dkv,buffer,sizeof(len_t));
					read(kv->fd_dkv,buffer,sizeof(len_t));
					if (strcmp(buffer,bufNum)==0) {
						lseek(kv->fd_dkv,-sizeof(len_t)-sizeof(len_t)-1,SEEK_CUR);
						char *temp = malloc(1);
						temp = "0";
						write(kv->fd_dkv,temp,1); //Changement du int d'existence
						lseek(kv->fd_blk,head_blk+blk->numExBlk*4096+sizeof(len_t),SEEK_SET);
						read(kv->fd_blk,buffer,sizeof(len_t));
						int nbrEntr = atoi(buffer);
						char *newEnt = malloc(sizeof(len_t));
						sprintf(newEnt,"%d",nbrEntr);
						lseek(kv->fd_blk,-1,SEEK_CUR);
						write(kv->fd_dkv,newEnt,sizeof(len_t)); //Changement du nbr d'entrée
						free(buffer);
						free(bufNum);
						return 1;
					}
				}
			}
		}
	}
	return 0;
}

int kv_del (KV *kv, const kv_datum *key) {
	len_t tmp;
	int n,i;
	char *bufNum = malloc(sizeof(len_t));
	char *buffer = malloc(TAILLE_MAX);
	BLK *blk = malloc(sizeof(BLK));

	switch (kv->hash) {
		case 0: tmp = hash_0(key->ptr);
						break;

		case 1:tmp = hash_1(key->ptr);
						break;

		case 2:tmp = hash_2(key->ptr);
						break;

		default : break;
	}

	lseek(kv->fd_h,head_h+(tmp*sizeof(len_t)),SEEK_SET);
	n=read(kv->fd_h,bufNum,sizeof(len_t));
	//On vient de récupérer le numéro du bloc correspondant à la valeur hachée
	if (n==0) { //Si trou
		errno=ENOENT;
		perror("Clé inexistente");
		return -1;
	}

	tmp = atoi(bufNum);
	lseek(kv->fd_blk,head_blk+tmp*4096,SEEK_SET);
	/*Les blocs commencent à 1 */

	/* code pour gérer les bloc */
	n=read(kv->fd_blk,bufNum,sizeof(len_t)); //On lit le num du bloc
	blk->numBlk = atoi(bufNum);
	n=read(kv->fd_blk,bufNum,sizeof(len_t)); //nbr d'entrée
	blk->nbrEnt = atoi(bufNum);
	n=read(kv->fd_blk,bufNum,sizeof(len_t)); //existence bloc suivant
	blk->numExBlk = atoi(bufNum);
	n=read(kv->fd_blk,bufNum,sizeof(len_t)); //num bloc suivant (0 par défaut)
	blk->nextBlk = atoi(bufNum);

	for(i=0;i<blk->nbrEnt;i++) {
		n=read(kv->fd_h,bufNum,sizeof(len_t));
		if (strcmp(bufNum, (char *)key->ptr)==0) {
			while ( (n=read(kv->fd_dkv,buffer,1)>0) ) {
				if (strcmp(buffer,"1")) {
					read(kv->fd_dkv,buffer,sizeof(len_t));
					read(kv->fd_dkv,buffer,sizeof(len_t));
					if (strcmp(buffer,bufNum)==0) {
						lseek(kv->fd_dkv,-sizeof(len_t)-sizeof(int)-1,SEEK_CUR);
						char *temp = malloc(1);
						temp = "0";
						write(kv->fd_dkv,temp,1); //Changement du int d'existence
						lseek(kv->fd_blk,head_blk+(tmp-1)*4096+sizeof(len_t),SEEK_SET);
						read(kv->fd_blk,buffer,sizeof(len_t));
						int nbrEntr = atoi(buffer);
						char *newEnt = malloc(sizeof(len_t));
						sprintf(newEnt,"%d",nbrEntr);
						lseek(kv->fd_blk,-1,SEEK_CUR);
						write(kv->fd_dkv,newEnt,sizeof(len_t)); //Changement du nbr d'entrée
						free(buffer);
						free(bufNum);
						return 0;
					}
				}
			}
		}
	}

	int t;

	/* Boucle qui va gérer successivement les différents blocs */
	while(blk->nextBlk!=0) {
		if ( (t=del(kv,blk,key)) == 1 ) {
			free(blk);
			free(buffer);
			free(bufNum);
			return 0;
		}
	}

	errno=ENOENT;
	perror("Clé inexistente");
	return -1;
}




/* On suppose que le pointeur est situé juste après
 * l'en-tête du fichier .dkv grâce à la fonction kv_next
 */
int kv_next (KV *kv, kv_datum *key, kv_datum *val) {
	int n, compt, lg;
	len_t offset;
	char *buf = malloc(TAILLE_MAX);
	if (key==NULL)
		key=malloc(sizeof(kv_datum));
	if (val==NULL)
		val=malloc(sizeof(kv_datum));

	while ( (n=read(kv->fd_dkv,buf,1))!=0  ) { //On  lit la longueur de la clé dans le fichier .kv
		compt = atoi(buf); //On récupère la valeur 0/1
		if ( compt==1 ) { //Si il y a bien un couple stocké
			lseek(kv->fd_dkv,sizeof(len_t),SEEK_CUR); //On se place à l'offset
			n=read(kv->fd_dkv,buf,sizeof(len_t)); //On lit l'offset
			offset = atoi(buf);
			lseek(kv->fd_kv,offset,SEEK_SET); //Début de la cellule
			n=read(kv->fd_dkv,buf,sizeof(len_t));//lecture longueur clé
			lg = atoi(buf);
			key->len = (len_t)lg;
			n=read(kv->fd_dkv,buf,lg);//Récupération de la clé
			key->ptr = (void *)buf;
			n=read(kv->fd_dkv,buf,sizeof(len_t));//Lecture longeur valeur
			lg = atoi(buf);
			val->len = (len_t)lg;
			n=read(kv->fd_dkv,buf,lg);
			val->ptr = (void *)buf;
			free(buf);
			return 1;
		}
		else
			lseek(kv->fd_dkv,sizeof(len_t)+sizeof(len_t),SEEK_CUR);
			/* On se replace au début de la prochaine cellule si la cellule précédente
			   est vide */
	}
	free(buf);
	return 0;
}

int main() {

	return 0;
}
