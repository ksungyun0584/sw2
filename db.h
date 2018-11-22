typedef struct listnode {
	int keylen;
	int vallen;
	char* key;
	char* value;
}listnode;

typedef struct meta {
	int keylen;
	char* key;
	int fnum;
	int fplace;
	struct meta* ptr;
}meta;

typedef struct db {
	listnode *hashtable;
	meta **mt;
	int size;
	int filenum;
	int wordcnt;
}db_t;

db_t* db_open(int size);
void db_close(db_t* db);

void db_put(db_t* db, char* key, int keylen, char* val, int vallen);
/* Returns NULL if not found. A malloc()ed array otherwise.
 * Stores the length of the array in *vallen */
char* db_get(db_t* db, char* key, int keylen, int* vallen);

int hashfunction(char* key, int keylen, int size);

void freetable(db_t* db);

void makefile(db_t* db);

char* searchfile(db_t* db, char* key, int keylen);
