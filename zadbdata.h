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

#include <limits.h>

#ifndef ZADBDATA_H_
#define ZADBDATA_H_

#define ZADB_DATA_NUM long long int
#define ZADB_DATA_TYPE unsigned short
#define ZADB_DATA_MAXSIZE USHRT_MAX

typedef void *zadbDataKey;
typedef void *zadbDataVal;

zadbDataVal zadbValNewStr(const char * val, ZADB_DATA_TYPE val_size);
zadbDataVal zadbValNewInt(ZADB_DATA_NUM num);

void zadbValGet(zadbDataVal d, char **str, ZADB_DATA_TYPE *str_size, ZADB_DATA_NUM *num, int *isString);
void zadbValSwap(zadbDataVal to, zadbDataVal from);
void zadbValFree(zadbDataVal d);

zadbDataKey zadbKeyNew(const char *table, ZADB_DATA_TYPE table_size, const char* key, ZADB_DATA_TYPE key_size, const char* field, ZADB_DATA_TYPE field_size, int ref);
void zadbKeyGet(zadbDataKey in, char **table, ZADB_DATA_TYPE *table_size, char **key, ZADB_DATA_TYPE *key_size, char ** field, ZADB_DATA_TYPE *field_size);
void zadbKeyFree(zadbDataKey d);

int zadbKeyFieldCompare(void *a, void *b);

#endif /* ZADBDATA_H_ */
