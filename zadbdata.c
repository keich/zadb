
/*

MIT License

Copyright (c) 2022 Alexander Zazhigin mykeich@yandex.ru

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "zadbdata.h"

typedef enum {
    ZADBDATASTR, ZADBDATAINT
} zadbType;

typedef struct zadbKey {
    ZADB_DATA_TYPE table_size;
    ZADB_DATA_TYPE key_size;
    ZADB_DATA_TYPE filed_size;
    char * table;
    char * key;
    char * field;
} zadbKey;

typedef struct zadbVal {
    zadbType type;
    ZADB_DATA_TYPE size;
} zadbVal;

typedef struct zadbValInt {
    zadbType type;
    ZADB_DATA_NUM num;
} zadbValInt;

long long malloccounter = 0;

zadbDataVal zadbValNewStr(const char * val, ZADB_DATA_TYPE val_size) {
    zadbVal *out = malloc(sizeof(zadbVal) + val_size * sizeof(char));
    malloccounter++;
    if (out == NULL) {
        perror("zadbVal_new filed");
        return NULL;
    }
    out->type = ZADBDATASTR;
    out->size = 0;
    if (val_size > 0 && val != NULL) {
        out->size = val_size;
        char *data = (char *) (out + 1);
        memcpy(data, val, val_size);
    }
    return (zadbDataVal) out;
}

zadbDataVal zadbValNewInt(ZADB_DATA_NUM num) {
    zadbValInt *out = malloc(sizeof(zadbValInt));
    malloccounter++;
    //printf("zadbValNewInt\n");
    if (out == NULL) {
        perror("zadbVal_new filed");
        return NULL;
    }
    out->type = ZADBDATAINT;
    out->num = num;
    return (zadbDataVal) out;
}

void zadbValGet(zadbDataVal d, char **str, ZADB_DATA_TYPE *str_size, ZADB_DATA_NUM *num, int *isString) {
    zadbVal *z = (zadbVal*) d;
    *str = (char *) (z + 1);
    *str_size = z->size;
    *isString = z->type == ZADBDATASTR;
    ZADB_DATA_NUM *tmp = (ZADB_DATA_NUM *) (z + 1);
    *num = *tmp;
    return;
}

void zadbValSwap(zadbDataVal to, zadbDataVal from) {
    zadbVal *a = (zadbVal*) to;
    zadbVal *b = (zadbVal*) from;
    zadbVal c;
    c = *a;
    *a = *b;
    *b = c;
}

void zadbValFree(zadbDataVal d) {
    //printf("zadbValFree\n");
    zadbVal *z = (zadbVal*) d;
    free(z);
    malloccounter--;
}

zadbKey tmpzadbKey;

zadbDataKey zadbKeyNew(const char *table, ZADB_DATA_TYPE table_size, const char* key, ZADB_DATA_TYPE key_size, const char* field, ZADB_DATA_TYPE field_size, int ref) {
    zadbKey *out;
    if (ref) {
        out = &tmpzadbKey;
        out->table = (char *) table;
        out->key = (char *) key;
        out->field = (char *) field;
    } else {
        malloccounter++;
        out = malloc(sizeof(zadbKey) + (table_size + key_size + field_size) * sizeof(char));
        if (out == NULL) {
            perror("zadbKey_new failed");
            return NULL;
        }
        out->table = (char *) (out + 1);
        out->key = out->table + table_size;
        out->field = out->key + key_size;
        memcpy(out->table, table, table_size);
        memcpy(out->key, key, key_size);
        memcpy(out->field, field, field_size);
    }

    out->key_size = key_size;
    out->filed_size = field_size;
    out->table_size = table_size;

    return (zadbDataKey) out;
}

void zadbKeyGet(zadbDataKey d, char **table, ZADB_DATA_TYPE *table_size, char **key, ZADB_DATA_TYPE *key_size, char ** field, ZADB_DATA_TYPE *field_size) {
    zadbKey *z = (zadbKey*) d;

    *table = z->table;
    *table_size = z->table_size;
    *key = z->key;
    *key_size = z->key_size;
    *field = z->field;
    *field_size = z->filed_size;
}

void zadbKeyFree(zadbDataKey d) {
    zadbKey *z = (zadbKey*) d;
    if (z->table == (char *) (z + 1)) {
        free(z);
        malloccounter--;
    }
}

int zadbKeyCompareStr(const char *key1, const size_t key1_size, const char *key2, const size_t key2_size) {
    if (key1_size < key2_size) {
        return -1;
    }
    if (key1_size > key2_size) {
        return 1;
    }
    return memcmp(key1, key2, key1_size);
}

int zadbKeyFieldCompare(void *a, void *b) {
    zadbKey *key1 = (zadbKey*) a;
    zadbKey *key2 = (zadbKey*) b;

    int ret = zadbKeyCompareStr(key1->table, key1->table_size, key2->table, key2->table_size);
    if (ret) {
        return ret;
    }

    ret = zadbKeyCompareStr(key1->key, key1->key_size, key2->key, key2->key_size);
    if (ret) {
        return ret;
    }

    return zadbKeyCompareStr(key1->field, key1->filed_size, key2->field, key2->filed_size);
}
