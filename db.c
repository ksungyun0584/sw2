/*-----------------------------------------------------------
 *
 * SSE2033: System Software Experiment 2 (Fall 2018)
 *
 * Skeleton code for PA#1
 * 
 * CSLab, Sungkyunkwan University
 *
 * Student Id   : 2013312757
 * Student Name : Kim Sung Yun
 *
 *-----------------------------------------------------------
 */
#include <unistd.h>
#include <fcntl.h>
#include "db.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>

#define BUF_SIZE 1024
int c=0;
db_t* db_open(int size) {
	db_t* db = (db_t*)malloc(sizeof(db_t));
	db->size = size;
	db->filenum = 0;
	db->wordcnt = 0;
	db->hashtable = (listnode*)malloc(sizeof(listnode)*size);
	db->mt = (meta**)malloc(sizeof(meta*)*size);
	for(int i=0;i<size;i++){
		db->mt[i] = (meta*)malloc(sizeof(meta));
		db->mt[i]->key = (char*)malloc(sizeof(char*));
		db->mt[i]->ptr = NULL;
	}

	if(fork()==0){
		execl("/bin/mkdir","mkdir","db",NULL);
	}
	wait(NULL);
	return db;
}

void db_close(db_t* db) {
	meta* mnode1;
	meta* mnode2;
	makefile(db);
	for(int i=0;i<db->size;i++){
		mnode1 = db->mt[i]->ptr;
		while(mnode1 != NULL){
			mnode2 = mnode1;
			free(mnode2->key);
			free(mnode2);
			mnode1 = mnode1->ptr;
		}
		free(db->mt[i]->key);
		free(db->mt[i]);
	}
	free(db->hashtable);
	free(db->mt);
	free(db);
}

void db_put(db_t* db, char* key, int keylen, char* val, int vallen) {
	int i,j;
	/****** search table ******/
	for(i=0;i<db->wordcnt;i++){
		if(strcmp(key,db->hashtable[i].key) == 0){
			*((int*)db->hashtable[i].value) = *((int*)val);
			break;
		}		
	}
	if(i==db->wordcnt){
		db->hashtable[i].key = (char*)malloc(sizeof(char)*(keylen+1));
		db->hashtable[i].value = (char*)malloc(sizeof(char*));
		for(j=0;j<keylen;j++){
			*(db->hashtable[i].key+j) = *(key+j); 
		}
		*(db->hashtable[i].key+j) = 0;
		*((int*)db->hashtable[i].value) = *((int*)val);
		db->hashtable[i].keylen = keylen;
		db->hashtable[i].vallen = vallen;
		db->wordcnt++;
	}
	/**************************/
	if(db->wordcnt == db->size){
		db->wordcnt = 0;
		makefile(db);
		freetable(db);
		db->filenum++;
	}
}

/* Returns NULL if not found. A malloc()ed array otherwise.
 * Stores the length of the array in *vallen */
char* db_get(db_t* db, char* key, int keylen, int* vallen) {
	char* value = NULL;
	int i;
	for(i=0;i<db->wordcnt;i++){
		if(strcmp(key,db->hashtable[i].key) == 0){
			value = (char*)malloc(sizeof(char*));
			*((int*)value) = *((int*)db->hashtable[i].value);
			return value;
		}		
	}
	if(i==db->wordcnt){
		return searchfile(db, key, keylen);
	}
	return value;
}

char* searchfile(db_t* db, char* key, int keylen){
	char* buf = (char*)malloc(sizeof(char)*BUF_SIZE);
	char v[4];
	char* value = NULL;
	int fd,fnum,place;
	int hash = hashfunction(key,keylen,db->size);

	meta* mnode;
	mnode = db->mt[hash];
	while(1){
		if(strcmp(mnode->key,key) == 0){
			fnum = mnode->fnum;
			place = mnode->fplace;
			break;
		}
		else if(mnode->ptr == NULL){
			free(buf);
			return value;
		}
		mnode = mnode->ptr;
	}

	sprintf(buf,"./db/file-%d.txt",fnum);
	fd = open(buf,O_RDWR);
	lseek(fd,place,SEEK_SET);
	if(read(fd,&v,4)<0) exit(0);
	close(fd);
	value = (char*)malloc(sizeof(char*));
	for(int l=0;l<4;l++){
		value[l] = *(v+l);
	}
	free(buf);
	return value;
}

void makefile(db_t* db) {
	meta* mnode;
	char* buf = (char*)malloc(sizeof(char)*BUF_SIZE);
	int place;
	int fd,j;
	sprintf(buf,"./db/file-%d.txt",db->filenum);
	fd = open(buf,O_CREAT | O_RDWR,0644);
	for(int i=0;i<db->size;i++){
		if(write(fd,(char*)&db->hashtable[i].keylen,4)<0) continue;
		if(write(fd,(char*)&db->hashtable[i].vallen,4)<0) continue;
		if(write(fd,db->hashtable[i].key,db->hashtable[i].keylen)<0) continue;
		place = lseek(fd,0,SEEK_CUR);
		if(write(fd,db->hashtable[i].value,4)<0) continue;

		mnode = db->mt[hashfunction(db->hashtable[i].key,db->hashtable[i].keylen,db->size)];
		while(1){
			if(strcmp(db->hashtable[i].key,mnode->key) == 0){
				mnode->fnum = db->filenum;
				mnode->fplace = place;
				break;
			}
			else if(mnode->ptr == NULL){
				meta* newnode = (meta*)malloc(sizeof(meta));
				newnode->key = (char*)malloc(sizeof(char)*(db->hashtable[i].keylen+1));

				newnode->fnum = db->filenum;
				newnode->fplace = place;
				newnode->keylen = db->hashtable[i].keylen;
				for(j=0;j<db->hashtable[i].keylen;j++){
					*(newnode->key+j) = *(db->hashtable[i].key+j);
				}
				*(newnode->key+j) = 0;
				newnode->ptr = NULL;
				mnode->ptr = newnode;
				break;
			}
			mnode = mnode->ptr;				
		}
	}
	free(buf);
	close(fd);
}

void freetable(db_t* db){
	for(int i=0;i<db->size;i++){
		db->hashtable[i].key[0] = 0;
		db->hashtable[i].value[0] = 0;
		free(db->hashtable[i].key);
		free(db->hashtable[i].value);		
	}
}

int hashfunction(char* key, int keylen, int size) {
	int hash = 0;
	for(int i=0;i<keylen;i++) {
		hash = hash + *(key + i);
		hash = hash * (size + 1);
		if(hash < 0) hash = hash * (-1);
	}
	hash = hash / (size+1);
	hash = hash % size;
	return hash;
}
