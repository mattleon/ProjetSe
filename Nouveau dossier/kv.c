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
#define head_blk 5
#define head_kv 1
#define head_dkv 5

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
 int numBlk; //Numéro du bloc
 int nbrEnt; //Max 1020
 int numExBlk; //Existence d'un bloc suivant ou non (0 si n'existe pas)
 int nextBlk; //Num du prochain bloc (0 si n'existe pas)
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



/* Structure blk du .blk :
 * En-tête contient quatre infos :
 * le nombre d'entrée (0<=n<=1020)
 * Le numéro du prochain bloc
 * 0 si le prochain bloc n'existe pas, sinon 1
 * le numéro du bloc
 */

 /* On supose que l'offset donné en argument est l'offset situé
  * au début du descripteur à monter, donc au 0/1
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
 		lseek(kv->fd_dkv,2*(-1-2*sizeof(len_t)),SEEK_CUR);//On se place à la cellule précédente
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
 	int n, nb_cell,nb,i;
 	unsigned int off;
 	lseek(kv->fd_dkv,sizeof(len_t),SEEK_SET);
 	n=read(kv->fd_dkv,buffer,sizeof(len_t));//On récupère le nombre de cellule
	if (n==-1) {
		perror("read");
		return -1;
	}
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

/* Met dans la première cellule libre dkv qui a une taille suffisante
 * prend en argument l'offset correspondant à l'offset dans .blk
 * Renvoie 0 en cas d'échec, sinon1
 */
len_t first_fit(KV *kv, const kv_datum *key, const kv_datum *val, len_t offset) {
	unsigned int nb_cell,i,vali=0,off,lg_tot,off1;
	int n;
	char *buffer = malloc(sizeof(len_t));
	lseek(kv->fd_dkv,sizeof(len_t),SEEK_SET);
	n=read(kv->fd_dkv,buffer,sizeof(len_t));
	if (n==-1) {
		perror("read");
		return -1;
	}
	nb_cell = atoi(buffer);

	for(i=0;i<nb_cell;i++) {
		n=read(kv->fd_dkv,buffer,1);//Lecture bit libre ou pas
		if ( atoi(buffer)==0) {
			n=read(kv->fd_dkv,buffer,sizeof(len_t));//lecture longueur place
			if ( (unsigned int)atoi(buffer)>=(2*sizeof(len_t)+key->len+val->len)) {
				vali=1;
				lg_tot=atoi(buffer);
				break;
			}
		}
		lseek(kv->fd_dkv,2*sizeof(len_t),SEEK_CUR);//On se place à la cellule suivante
	}

	if (vali==0) {//Si aucune cellule vide, alors renvoyer 0
		return 0;
	}

	n=read(kv->fd_dkv,buffer,sizeof(len_t));//Lecture offset
	/* Le pointeur est donc au niveau de la cellule suivante
	 * On retient l'offset en mémoire et on fait un décalage vers le base
	 */
	off = lseek(kv->fd_dkv,0,SEEK_CUR);
	lseek(kv->fd_blk,offset,SEEK_SET);
	write(kv->fd_blk,buffer,sizeof(len_t));
	lseek(kv->fd_kv,atoi(buffer),SEEK_SET);
	sprintf(buffer,"%d",key->len);
	write(kv->fd_kv,buffer,sizeof(len_t));//Ecriture longueur clé
	write(kv->fd_kv,(char *)key->ptr,sizeof(len_t));//Ecriture clé
	sprintf(buffer,"%d",val->len);
	write(kv->fd_kv,buffer,sizeof(len_t));//Ecriture longueur val
	write(kv->fd_kv,(char *)val->ptr,key->len);//Ecriture val
	off1 = lseek(kv->fd_kv,0,SEEK_CUR);
	//pointeur dans le .kv à la fin de la "cellule"
	//Faire vérif taille cellule et séparation si besoin est.
	if ( lg_tot>=( (2*sizeof(len_t)+key->len+val->len)+(2*sizeof(len_t)+1) )) {
		lg_tot = lg_tot-2*sizeof(len_t)-key->len-val->len;
		decale_bas(kv,off);
		lseek(kv->fd_dkv,off,SEEK_SET);
		sprintf(buffer,"%d",0);
		write(kv->fd_blk,buffer,1);//Ecriture bit descripteur
		sprintf(buffer,"%d",lg_tot);
		write(kv->fd_dkv,buffer,sizeof(len_t));//Ecriture longueur de la case restante
		sprintf(buffer,"%d",off1);
		write(kv->fd_dkv,buffer,sizeof(len_t));//Ecriture offset du .kv
	}

	return 1;
}

/* L'offset donné en argument correspond à l'offset dans le blk
 * Là où doit être écrit l'offset du kv
 * Renvoie 0 si échec, sinon 1
 */
len_t worst_fit(KV *kv, const kv_datum *key, const kv_datum *val, len_t offset) {
	char *buffer = malloc(TAILLE_MIN);
	int n,i,nb_cell,off,off1;
	int lg=0;
	lseek(kv->fd_dkv,1,SEEK_SET);
  int temp;
	n=read(kv->fd_dkv,&temp,sizeof(len_t));//Lecture nombre cellule
  printf("%d\n",temp);
	if (n==-1) {
		perror("read");
		return -1;
	}
	nb_cell=atoi(buffer);
  printf("%d\n",nb_cell);
	for(i=0;i<nb_cell;i++) {
		n=read(kv->fd_dkv,buffer,1);//Lecture premier bit descripteur
		if ( atoi(buffer)==0) {
			n=read(kv->fd_dkv,buffer,sizeof(len_t));//Lecture de la longueur de la case
			if ( lg > atoi(buffer) ){
				lg = atoi(buffer);//On choisit la plus grand longueur
				off = lseek(kv->fd_dkv,0,SEEK_CUR);//On retient la position actuelle
			}
		}
		lseek(kv->fd_dkv,sizeof(len_t),SEEK_CUR);//On passe l'offset
	}

	lseek(kv->fd_dkv,off,SEEK_CUR);//On se replace à l'offset de la plus grand longueur
	if ((unsigned int)lg < (2*sizeof(len_t)+val->len+key->len) )//Verification de la place
		return 0;

	n=read(kv->fd_dkv,buffer,sizeof(len_t));//On lit l'offset
	off=atoi(buffer);
	lseek(kv->fd_blk,offset,SEEK_SET);//On se place à l'offset donné en arg
	sprintf(buffer,"%d",off);
	write(kv->fd_blk,buffer,sizeof(len_t));//Ecriture de l'offset du kv dans blk
	lseek(kv->fd_kv,off,SEEK_SET);//On se place dans le kv
	sprintf(buffer,"%d",key->len);
	write(kv->fd_kv,buffer,sizeof(len_t));//Ecriture longueur clé
	write(kv->fd_kv,(char *)key->ptr,key->len);//Ecriture clé
	sprintf(buffer,"%d",val->len);
	write(kv->fd_kv,buffer,sizeof(len_t));//Ecriture longueur val
	write(kv->fd_kv,(char *)val->ptr,val->len);//Ecriture val
	off1 = lseek(kv->fd_kv,0,SEEK_CUR);//Récupérer l'adresse de la nouvelle future case au cas-où
	//Le pointeur est donc à la fin de la "case"

	if ( (unsigned int)lg >= (4*sizeof(len_t)+1+key->len+val->len) ) {//Si taille suffisante pour nouvelle case
		decale_bas(kv,off);//Faire la place pour la nouvelle case
		lseek(kv->fd_dkv,off,SEEK_SET);//On se place au début de la nouvelle case
		sprintf(buffer,"%d",0);
		write(kv->fd_dkv,buffer,1);//Ecriture du bit d'emplacement libre
		sprintf(buffer,"%ld",lg-(2*sizeof(len_t)-key->len-val->len));
		write(kv->fd_dkv,buffer,sizeof(len_t));//Ecritue de la longueur restante
		sprintf(buffer,"%d",off1);
		write(kv->fd_dkv,buffer,sizeof(len_t));//Ecriture de l'offset
	}

	return 1;
}


len_t best_fit(KV *kv, const kv_datum *key, const kv_datum *val, len_t offset) {
	char *buffer = malloc(TAILLE_MIN);
	int n,i,nb_cell,off,off1,j=0;
	int lg=0;
	lseek(kv->fd_dkv,sizeof(len_t),SEEK_SET);
	n=read(kv->fd_dkv,buffer,sizeof(len_t));//Lecture nombre cellule
	if (n==-1) {
		perror("read");
		return -1;
	}
	nb_cell=atoi(buffer);

	while (j!=nb_cell) {//Boucle pour récupérer la première longueur d'une cell vide
		n=read(kv->fd_dkv,buffer,1);//Lecture premier bit descripteur
		if ( atoi(buffer)==0) {
			n=read(kv->fd_dkv,buffer,sizeof(len_t));//Lecture de la longueur de la case
			lg = atoi(buffer);
			off = lseek(kv->fd_dkv,0,SEEK_CUR);//On retient la position actuelle
			break;
		}
		lseek(kv->fd_dkv,2*sizeof(len_t),SEEK_CUR);
		j++;
	}

	for(i=0;i<nb_cell;i++) {
		n=read(kv->fd_dkv,buffer,1);//Lecture premier bit descripteur
		if ( atoi(buffer)==0) {
			n=read(kv->fd_dkv,buffer,sizeof(len_t));//Lecture de la longueur de la case
			if ( (unsigned int)atoi(buffer) > (unsigned int)(2*sizeof(len_t)+key->len+val->len) ){//Si longueur suffisante
				if ( atoi(buffer)<lg) {//Si longueur plus petite que longueur actuelle en mém
					lg = atoi(buffer);//
					off = lseek(kv->fd_dkv,0,SEEK_CUR);//On retient la position actuelle
				}
			}
		}
		lseek(kv->fd_dkv,sizeof(len_t),SEEK_CUR);//On passe l'offset
	}

	lseek(kv->fd_dkv,off,SEEK_CUR);//On se replace à l'offset de la plus grand longueur
	if ((unsigned int)lg < (2*sizeof(len_t)+val->len+key->len) )//Verification de la place
		return 0;

	n=read(kv->fd_dkv,buffer,sizeof(len_t));//On lit l'offset
	off=atoi(buffer);
	lseek(kv->fd_blk,offset,SEEK_SET);//On se place à l'offset donné en arg
	sprintf(buffer,"%d",off);
	write(kv->fd_blk,buffer,sizeof(len_t));//Ecriture de l'offset du kv dans blk
	lseek(kv->fd_kv,off,SEEK_SET);//On se place dans le kv
	sprintf(buffer,"%d",key->len);
	write(kv->fd_kv,buffer,sizeof(len_t));//Ecriture longueur clé
	write(kv->fd_kv,(char *)key->ptr,key->len);//Ecriture clé
	sprintf(buffer,"%d",val->len);
	write(kv->fd_kv,buffer,sizeof(len_t));//Ecriture longueur val
	write(kv->fd_kv,(char *)val->ptr,val->len);//Ecriture val
	off1 = lseek(kv->fd_kv,0,SEEK_CUR);//Récupérer l'adresse de la nouvelle future case au cas-où
	//Le pointeur est donc à la fin de la "case"

	if ( (unsigned int)lg >= (4*sizeof(len_t)+1+key->len+val->len) ) {
		//Si taille suffisante pour nouvelle case
		decale_bas(kv,off);//Faire la place pour la nouvelle case
		lseek(kv->fd_dkv,off,SEEK_SET);//On se place au début de la nouvelle case
		sprintf(buffer,"%d",0);
		write(kv->fd_dkv,buffer,1);//Ecriture du bit d'emplacement libre
		sprintf(buffer,"%ld",lg-(2*sizeof(len_t)-key->len-val->len));
		write(kv->fd_dkv,buffer,sizeof(len_t));//Ecritue de la longueur restante
		sprintf(buffer,"%d",off1);
		write(kv->fd_dkv,buffer,sizeof(len_t));//Ecriture de l'offset
	}

	return 1;


	return 0;
}

len_t hash_0(const kv_datum *a){
 	unsigned int sum=0;
 	unsigned int i;
 	for(i=0; i<a->len; i++)
 		sum=((char*)a->ptr)[i];
 	return sum%999983;
}

len_t hash_1(const kv_datum *a){
 	unsigned int sum=0;
 	unsigned int i=0;
  char *str = (char *)a->ptr;
  printf("test1\n");
 	for(i=0; i<a->len; i++)
 		sum=sum+str[i];
  printf("test2\n");
 	return sum%4096;
}

len_t hash_2(const kv_datum *a){
 	unsigned int sum=0;
 	unsigned int i=0;
 	for(i=0; i<a->len; i++)
 		sum=(sum+i+1)*((char*)a->ptr)[i];
 	return sum%4096;
}

KV *kv_open (const char *dbname, const char *mode, int hidx, alloc_t alloc){
	KV *new = malloc(sizeof(KV));
	new->alloc = alloc;
	len_t var;
	char *extend = malloc(TAILLE_MIN);
	char *file = malloc(TAILLE_MIN);
	int fd1, fd2, fd3, fd4;
	new->hash=hidx;
  printf("%d\n%d\n",hidx,new->hash);
	struct stat bufferfile;
	char * buff=malloc(TAILLE_MIN);
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
		lseek(fd1, 0, SEEK_SET);
		if(read(fd1, buff, 1)==-1)
			return NULL;
		if(buff[0]!=1)
			return NULL;
		new->fd_blk = fd1;

		extend = ".dkv";
		snprintf(file, PATH_MAX, "%s%s", dbname, extend);
		if((fd2 = open(file, O_RDONLY))==-1)
			return NULL;
		lseek(fd2, 0, SEEK_SET);
		if(read(fd2, buff, 1)==-1)
			return NULL;
		if(buff[0]!=2)
			return NULL;
		new->fd_dkv = fd2;

		extend = ".kv";
		snprintf(file, PATH_MAX, "%s%s", dbname, extend);
		if((fd3 = open(file, O_RDONLY))==-1)
			return NULL;
		lseek(fd3, 0, SEEK_SET);
		if(read(fd3, buff, 1)==-1)
			return NULL;
		if(buff[0]!=3)
			return NULL;
		new->fd_kv= fd3;

		extend = ".h";
		snprintf(file, PATH_MAX, "%s%s", dbname, extend);
		if((fd4 = open(file, O_RDONLY))==-1)
			return NULL;
		lseek(fd4, 0, SEEK_SET);
		if(read(fd4, buff, 1)==-1)
			return NULL;
		if(buff[0]!=4)
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
			if(write(fd1, "1", 1)==-1)
				return NULL;
			var = 0;
			if(write(fd1, &var, sizeof(len_t))==-1)
				return NULL;
			if(close(fd1)==-1)
				return NULL;

			extend = ".dkv";
			snprintf(file, PATH_MAX, "%s%s", dbname, extend);
			if((fd2 = open(file, O_CREAT | O_WRONLY, 0600))==-1)
				return NULL;
			if(write(fd2, "2", 1)==-1)
				return NULL;
			var = 1;
			if(write(fd2, &var, sizeof(len_t))==-1)
				return NULL;
			if(write(fd2, "0", 1)==-1)
				return NULL;
			var = 4294967296-1;
			if(write(fd2, &var, sizeof(len_t))==-1)
				return NULL;
			var = 4;
			if(write(fd2, &var, sizeof(len_t))==-1)
				return NULL;
			if(close(fd2)==-1)
				return NULL;

			extend = ".kv";
			snprintf(file, PATH_MAX, "%s%s", dbname, extend);
			if((fd3 = open(file, O_CREAT | O_WRONLY, 0600))==-1)
				return NULL;
			if(write(fd3, "3", 1)==-1)
				return NULL;
			if(close(fd3)==-1)
				return NULL;

			extend = ".h";
			snprintf(file, PATH_MAX, "%s%s", dbname, extend);
			if((fd4 = open(file, O_CREAT | O_WRONLY, 0600))==-1)
				return NULL;
			if(write(fd4, "4", 1)==-1)
				return NULL;
			if(write(fd4, &hidx, 1)==-1)
				return NULL;
			if(close(fd4)==-1)
				return NULL;
		}

		extend = ".blk";
		snprintf(file, PATH_MAX, "%s%s", dbname, extend);
		if((fd1 = open(file, O_RDWR))==-1)
			return NULL;
		lseek(fd1, 0, SEEK_SET);
		if(read(fd1, buff, 1)==-1)
			return NULL;
		if(atoi(buff)!=1)
			return NULL;
		new->fd_blk = fd1;

		extend = ".dkv";
		snprintf(file,PATH_MAX,"%s%s",dbname,extend);
		if((fd2 = open(file, O_RDWR))==-1)
			return NULL;
		lseek(fd2, 0, SEEK_SET);
		if(read(fd2, buff, 1)==-1)
			return NULL;
		if(atoi(buff)!=2)
			return NULL;
		new->fd_blk = fd2;

		extend = ".kv";
		snprintf(file,PATH_MAX,"%s%s",dbname,extend);
		if((fd3 = open(file,O_RDWR))==-1)
			return NULL;
		lseek(fd3, 0, SEEK_SET);
		if(read(fd3, buff, 1)==-1)
			return NULL;
		if(atoi(buff)!=3)
			return NULL;
		new->fd_kv= fd3;

		extend = ".h";
		snprintf(file,PATH_MAX,"%s%s",dbname,extend);
		if((fd4 = open(file,O_RDWR))==-1)
			return NULL;
		lseek(fd4, 0, SEEK_SET);
		if(read(fd4, buff, 1)==-1)
			return NULL;
		if(atoi(buff)!=4)
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
		lseek(fd1, 0, SEEK_SET);
		if(read(fd1, buff, 1)==-1)
			return NULL;
		if(atoi(buff)!=1)
			return NULL;
		new->fd_blk = fd1;

		extend = ".dkv";
		snprintf(file, PATH_MAX, "%s%s", dbname, extend);
		if((fd2 = open(file, O_TRUNC | O_WRONLY))==-1)
			return NULL;
		lseek(fd2, 0, SEEK_SET);
		if(read(fd2, buff, 1)==-1)
			return NULL;
		if(atoi(buff)!=2)
			return NULL;
		new->fd_dkv = fd2;

		extend = ".kv";
		snprintf(file,PATH_MAX,"%s%s",dbname,extend);
		if((fd3 = open(file, O_TRUNC | O_WRONLY))==-1)
			return NULL;
		lseek(fd3, 0, SEEK_SET);
		if(read(fd3, buff, 1)==-1)
			return NULL;
		if(atoi(buff)!=3)
			return NULL;
		new->fd_kv= fd3;

		extend = ".h";
		snprintf(file, PATH_MAX, "%s%s", dbname, extend);
		if((fd4 = open(file, O_TRUNC | O_WRONLY))==-1)
			return NULL;
		lseek(fd4, 0, SEEK_SET);
		if(read(fd4, buff, 1)==-1)
			return NULL;
		if(atoi(buff)!=4)
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
			if(write(fd1, "1", 1)==-1)
				return NULL;
			var=0;
			if(write(fd1, &var, sizeof(len_t))==-1)
				return NULL;
			if(close(fd1)==-1)
				return NULL;

			extend = ".dkv";
			snprintf(file,PATH_MAX,"%s%s",dbname,extend);
			if((fd2 = open(file, O_CREAT | O_WRONLY, 0600))==-1)
				return NULL;
			if(write(fd2, "2", 1)==-1)
				return NULL;
			len_t var=1;
			if(write(fd2, &var, sizeof(len_t))==-1)
				return NULL;
			if(write(fd2, "0", 1)==-1)
				return NULL;
			var= 4294967296-1;
			if(write(fd2, &var, sizeof(len_t))==-1)
				return NULL;
			var=4;
			if(write(fd2, &var, sizeof(len_t))==-1)
				return NULL;
			if(close(fd2)==-1)
				return NULL;

			extend = ".kv";
			snprintf(file, PATH_MAX, "%s%s", dbname, extend);
			if((fd3 = open(file, O_CREAT | O_WRONLY, 0600))==-1)
				return NULL;
			if(write(fd3, "3", 1)==-1)
				return NULL;
			if(close(fd3)==-1)
				return NULL;

			extend = ".h";
			snprintf(file, PATH_MAX, "%s%s", dbname, extend);
			if((fd4 = open(file, O_CREAT | O_WRONLY, 0600))==-1)
				return NULL;
			if(write(fd4, "4", 1)==-1)
				return NULL;
			var= hidx;
			if(write(fd4, &var, 1)==-1)
				return NULL;
			if(close(fd4)==-1)
				return NULL;
		}

		extend = ".blk";
		snprintf(file, PATH_MAX, "%s%s", dbname, extend);
		if((fd1 = open(file, O_TRUNC | O_RDWR))==-1)
			return NULL;
		if(write(fd1, "1", 1)==-1)
			return NULL;
		var= 0;
		if(write(fd1, &var, sizeof(len_t))==-1)
			return NULL;
		lseek(fd1, 0, SEEK_SET);
		if(read(fd1, buff, 1)==-1)
			return NULL;
		if(atoi(buff)!=1)
			return NULL;
		new->fd_blk = fd1;

		extend = ".dkv";
		snprintf(file, PATH_MAX, "%s%s", dbname, extend);
		if((fd2 = open(file, O_TRUNC | O_RDWR))==-1)
			return NULL;
		if(write(fd2, "2", 1)==-1)
			return NULL;
		var=1;
		if(write(fd2, &var, sizeof(len_t))==-1)
			return NULL;
		if(write(fd2, "0", 1)==-1)
			return NULL;
		var= 4294967296-1;
		if(write(fd2, &var, sizeof(len_t))==-1)
			return NULL;
		var=4;
		if(write(fd2, &var, sizeof(len_t))==-1)
			return NULL;
		lseek(fd2, 0, SEEK_SET);
		if(read(fd2, buff, 1)==-1)
			return NULL;
		if(atoi(buff)!=2)
			return NULL;
		new->fd_dkv = fd2;

		extend = ".kv";
		snprintf(file, PATH_MAX, "%s%s", dbname, extend);
		if((fd3 = open(file, O_TRUNC | O_RDWR))==-1)
			return NULL;
		if(write(fd3, "3", 1)==-1)
			return NULL;
		lseek(fd3, 0, SEEK_SET);
		if(read(fd3, buff, 1)==-1)
			return NULL;
		if(atoi(buff)!=3)
			return NULL;
		new->fd_kv= fd3;

		extend = ".h";
		snprintf(file, PATH_MAX, "%s%s", dbname, extend);
		if((fd4 = open(file, O_TRUNC | O_RDWR))==-1)
			return NULL;
		if(write(fd4, "4", 1)==-1)
			return NULL;
		var=hidx;
		if(write(fd4, &var, 1)==-1)
			return NULL;
		lseek(fd4, 0, SEEK_SET);
		if(read(fd4, buff, 1)==-1)
			return NULL;
		if(atoi(buff)!=4)
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
		perror("Close:");
		return -1;
	}
}



int kv_get (KV *kv, const kv_datum *key, kv_datum *val) {

	BLK *blk = malloc(sizeof(BLK));
	//lseek(kv->fd_kv,head_kv,SEEK_SET);
	lseek(kv->fd_dkv,head_dkv,SEEK_SET);//On se place au début du dkv (après l'en-tête)
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

	lseek(kv->fd_h,head_h+tmp*sizeof(len_t),SEEK_SET);//On se place à la valeur du hash
	n=read(kv->fd_h,buf,sizeof(len_t));//Adresse du bloc correspondant
	lseek(kv->fd_blk,atoi(buf),SEEK_SET);
	n=read(kv->fd_blk,buf,sizeof(len_t)); //Lecture num bloc
	blk->numBlk = atoi(buf);
	n=read(kv->fd_blk,buf,sizeof(len_t));//Lecture nombre entrée dans le bloc
	blk->nbrEnt = atoi(buf);
	n=read(kv->fd_blk,buf,sizeof(len_t));//Lecture existence bloc suivant
	blk->numExBlk = atoi(buf);
	n=read(kv->fd_blk,buf,sizeof(len_t));//Num du prochain bloc si existe (sinon 0)
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
			n=read(kv->fd_dkv,buf,sizeof(len_t));//lit la longueur de la clé
			lg = atoi(buf);
			n=read(kv->fd_dkv,buf,lg);//lecture de la clé
			if ( strcmp(buf,(char *)key->ptr)==0 ) {
				if (val==NULL) {
					n=read(kv->fd_dkv,buf,sizeof(len_t));//Lecture de la longueur de val
					lg = atoi(buf);
					n=read(kv->fd_dkv,buf,lg);
					val=malloc(sizeof(kv_datum));
					val->ptr = (void *)buf;
					val->len = lg;
					free(buf);
					return 1;
				}
				else {
					lseek(kv->fd_kv,sizeof(len_t),SEEK_CUR);
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
			n=read(kv->fd_blk,buf,sizeof(len_t)); //Lecture num bloc
			blk->numBlk = atoi(buf);
			n=read(kv->fd_blk,buf,sizeof(len_t));//Lecture nombre entrée dans le bloc
			blk->nbrEnt = atoi(buf);
			n=read(kv->fd_blk,buf,sizeof(len_t));//Lecture existence bloc suivant
			blk->numExBlk = atoi(buf);
			n=read(kv->fd_blk,buf,sizeof(len_t));//Num du prochain bloc si existe (sinon 0)
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

	int n,offset,tmp1;
  len_t tmp;
	char *buffer = malloc(sizeof(len_t));
	char *bufNum = malloc(TAILLE_MAX);
	BLK *blk = malloc(sizeof(BLK));
	switch (kv->hash) {
		case 0: tmp = hash_0(key);
						break;

		case 1: tmp = hash_1(key);
						break;

		case 2: tmp = hash_2(key);
						break;

		default : break;
	}

	lseek(kv->fd_h,head_h+tmp*sizeof(len_t),SEEK_SET);
  //On se met à la position correspondant à la position de l'offset
  //de la clé hachée

	n=read(kv->fd_h,buffer,sizeof(len_t));//lecture offset
	if (n==0) {//Si pas d'offset présent
		lseek(kv->fd_h,-sizeof(len_t),SEEK_SET);//retour à la position précédente
		lseek(kv->fd_blk,sizeof(len_t),SEEK_SET);//Position au nbr de blocs
		n=read(kv->fd_blk,buffer,sizeof(len_t));//Lecure nbr blocs
		lseek(kv->fd_blk,-sizeof(len_t),SEEK_CUR);//Retour en arrière
    int nbBloc = atoi(buffer) +1;
		write(kv->fd_blk,&nbBloc,sizeof(len_t));//Ecriture nombre de blocs +1
		offset = lseek(kv->fd_blk,head_blk + (nbBloc-1)*4096,SEEK_SET);
    //Placement au nouveau bloc avec -1 pour ne pas avoir un trou de 4096
    //Car nb trou commence à 1


		write(kv->fd_blk,&nbBloc,sizeof(len_t));//Ecriture numéro blocs
		tmp1 = 1;
		write(kv->fd_blk,&tmp1,sizeof(len_t));//Ecriture nombre entrée
		tmp1 = 0;
		write(kv->fd_blk,&tmp1,sizeof(len_t));//Ecriture ex bloc suivant
		tmp1 = 0;
		write(kv->fd_blk,&tmp1,sizeof(len_t));//Ecriture num bloc suivant
    //On est maintenant au début du bloc
		offset = lseek(kv->fd_blk,0,SEEK_CUR);

		switch(kv->alloc) {//En fonction de la fonction d'alloc
			case FIRST_FIT: first_fit(kv,key,val,offset);
											break;

			case WORST_FIT: worst_fit(kv,key,val,offset);
											break;

			case BEST_FIT: best_fit(kv,key,val,offset);
											break;

			default: break;
		}

	}
	else {//Si entrée dans .h, on suppose que le bloc dans blk est existant.
		offset = atoi(buffer);
		lseek(kv->fd_blk,offset,SEEK_SET);
		n=read(kv->fd_blk,bufNum,sizeof(len_t)); //On lit le num du bloc
		blk->numBlk = atoi(bufNum);
		n=read(kv->fd_blk,bufNum,sizeof(len_t)); //nbr d'entrée
		blk->nbrEnt = atoi(bufNum);
		n=read(kv->fd_blk,bufNum,sizeof(len_t)); //existence bloc suivant
		blk->numExBlk = atoi(bufNum);
		n=read(kv->fd_blk,bufNum,sizeof(len_t)); //num bloc suivant (0 par défaut)
		blk->nextBlk = atoi(bufNum);

		while (blk->numExBlk!=0) {//Parcours de tous les blocs correspondant à la clé
			lseek(kv->fd_blk,head_blk + 4096*(blk->nextBlk-1),SEEK_SET);//On se place au bloc suivant
      n=read(kv->fd_blk,bufNum,sizeof(len_t)); //On lit le num du bloc
  		blk->numBlk = atoi(bufNum);
  		n=read(kv->fd_blk,bufNum,sizeof(len_t)); //nbr d'entrée
  		blk->nbrEnt = atoi(bufNum);
  		n=read(kv->fd_blk,bufNum,sizeof(len_t)); //existence bloc suivant
  		blk->numExBlk = atoi(bufNum);
  		n=read(kv->fd_blk,bufNum,sizeof(len_t)); //num bloc suivant (0 par défaut)
  		blk->nextBlk = atoi(bufNum);
		}
    //On est ici au dernier bloc de la clé hachée, si tout va bien

    offset = lseek(kv->fd_blk,0,SEEK_CUR);
    //On se met à l'offset correspondant à la nouvelle entrée à écriture
    switch(kv->alloc) {//En fonction de la fonction d'alloc
			case FIRST_FIT: first_fit(kv,key,val,offset);
											break;

			case WORST_FIT: worst_fit(kv,key,val,offset);
											break;

			case BEST_FIT:best_fit(kv,key,val,offset);
											break;

			default: break;
		}

  }
	return 0;
}


/* Cette fonction place le pointeur dans les fichiers .kv et .dkv juste après
 * l'en-tête
 */
void kv_start (KV *kv) {
	lseek(kv->fd_dkv,head_dkv,SEEK_SET);
}


/* On suppose que le pointeur est situé juste avant le 0
 * dans le descripteur que l'on vient de libérer
 * On suppose aussi que la fonction fusionne étant appelée à chaque del,
 * Il n'y a que deux cellules à fusionner, soit celle-là et celle du dessus,
 * soit celle-là et celle du dessous.
 */

void fusionne_cel(KV *kv) {
	int n,lg,offset;
	char *buffer = malloc(sizeof(len_t));
	lseek(kv->fd_dkv,-1-2*sizeof(len_t),SEEK_CUR);
	n=read(kv->fd_dkv,buffer,1);//Lecture bit descripteur au dessus
	if (n==-1) {
		perror("read");
	}
	if ( atoi(buffer)==0) {//Si cellule du dessus vide
		n=read(kv->fd_dkv,buffer,sizeof(len_t));//Longueur cellule au dessus
		lg = atoi(buffer);
		lseek(kv->fd_dkv,1+sizeof(len_t),SEEK_CUR);//longueur cellule à modifier
		n=read(kv->fd_dkv,buffer,sizeof(len_t));
		lg+=atoi(buffer);//On est après la longueur de la cellule "mère"
		lseek(kv->fd_dkv,-1-3*sizeof(len_t),SEEK_CUR);
		sprintf(buffer,"%d",lg);
		write(kv->fd_dkv,buffer,sizeof(len_t));//écriture de la nouvelle Longueur
		lseek(kv->fd_dkv,sizeof(len_t),SEEK_CUR);
		offset = lseek(kv->fd_dkv,0,SEEK_CUR);
		/* On est placé après la "nouvelle" cellule, au début de l'ancienne */
		decale_haut(kv,offset);
	}
	lseek(kv->fd_dkv,1+4*sizeof(len_t),SEEK_CUR);
	/* On était placé au bit du descripteur précédent, si on est ici on est pas allé dans le if
	 * On se place alors au début de la cellule après la cellule à fusionner
	 */
	 n=read(kv->fd_dkv,buffer,1);
	 if ( atoi(buffer)==0) {
		 n=read(kv->fd_dkv,buffer,sizeof(len_t));//Lecture longueur cellule
		 lseek(kv->fd_dkv,-1-3*sizeof(len_t),SEEK_CUR);
		 lg=atoi(buffer);
		 n=read(kv->fd_dkv,buffer,sizeof(len_t));
		 lg+=atoi(buffer);
		 lseek(kv->fd_dkv,-sizeof(len_t),SEEK_CUR);
		 sprintf(buffer,"%d",lg);
		 write(kv->fd_dkv,buffer,sizeof(len_t));
		 lseek(kv->fd_dkv,sizeof(len_t),SEEK_CUR);
		 offset = lseek(kv->fd_dkv,0,SEEK_CUR);
		 decale_haut(kv,offset);
	 }
}


int decale_blk(KV *kv, BLK *blk, int offset) {
	int i,j=0,off,n;
	char *buffer = malloc(TAILLE_MAX);

	lseek(kv->fd_blk,head_blk + (blk->numBlk)*4096,SEEK_SET);

	for(i=0;i<blk->nbrEnt;i++) {
		j++;
		n=read(kv->fd_blk,buffer,sizeof(len_t));//Lecture des offsets présents dans le bloc
		if (n==-1) {
			perror("read");
			return -1;
		}

		if ( atoi(buffer)==offset ) {//Si les offsets correspondent
			off=lseek(kv->fd_blk,0,SEEK_CUR);
			n=read(kv->fd_blk,buffer,blk->nbrEnt-j);//On lit tous les offsets restants
			lseek(kv->fd_blk,off-sizeof(len_t),SEEK_SET);//On se replace à l'offset - sizeof(len_t)
			write(kv->fd_blk,buffer,sizeof(len_t)*blk->nbrEnt-j);
			return 1;
		}
	}

	return 0;
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
			while ( (n=read(kv->fd_dkv,buffer,1)>0) ) {//lecture bit existence
				if (strcmp(buffer,"1")) {
					read(kv->fd_dkv,buffer,sizeof(len_t));//lecture longueur
					read(kv->fd_dkv,buffer,sizeof(len_t));//lecture offset
					if (strcmp(buffer,bufNum)==0) {//Comparaison offset - offset
						lseek(kv->fd_dkv,-sizeof(len_t)-sizeof(len_t)-1,SEEK_CUR);//Debut cellule
						char *temp = malloc(TAILLE_MIN);
						sprintf(temp,"%d",0);
						write(kv->fd_dkv,temp,1); //Changement du int d'existence
						lseek(kv->fd_blk,head_blk+blk->numExBlk*4096+sizeof(len_t),SEEK_SET);
						/* On se place dans le blk
						 * au niveau du bloc
						 */
						read(kv->fd_blk,buffer,sizeof(len_t));
						int nbrEntr = atoi(buffer);
						char *newEnt = malloc(sizeof(len_t));
						sprintf(newEnt,"%d",nbrEntr-1);//On enlève 1 au nombre d'entrées dans le bloc
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
		case 0: tmp = hash_0(key);
						break;

		case 1:tmp = hash_1(key);
						break;

		case 2:tmp = hash_2(key);
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
  KV *kv = kv_open("test", "r+",0, 1);
  kv_datum *key = malloc(sizeof(kv_datum));
  kv_datum *val = malloc(sizeof(kv_datum));
  key->len = 4;
  key->ptr = "test";
  val->len = 4;
  val->ptr = "1234";
  char buffer[10];
  lseek(kv->fd_dkv,1,SEEK_SET);
  read(kv->fd_dkv,buffer,sizeof(len_t));
  printf("%s\n",buffer);
  //kv_put(kv,key,val);
  free(key);
  free(val);
  kv_close(kv);
  free(kv);
  return 0;
}
