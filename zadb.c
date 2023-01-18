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
#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>
#include <assert.h>
#include <poll.h>
#include <unistd.h>
#include <netinet/ip.h>
#include <sys/select.h>
#include <errno.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include "rbtr.h"
#include "zadbdata.h"

#define DEFAULT_PORT 7000
#define SOCKET_CLIENT_BUFFER 256000


RbtHandle *rbtHandle;
lua_State *luaState;
lua_State *luaStateThread;

struct timeval g_starttime, g_endtime;
long long timediff = 0;
long long db_stat_set = 0;
long long db_stat_get = 0;
long long db_stat_upd = 0;
long long db_stat_del = 0;

/*
 * timeval_diff used for get difference between two timeeval.
 * Used with gettimeofday
 */
long long timeval_diff(struct timeval *end_time, struct timeval *start_time) {
    static struct timeval temp_diff;
    temp_diff.tv_sec = end_time->tv_sec - start_time->tv_sec;
    temp_diff.tv_usec = end_time->tv_usec - start_time->tv_usec;
    if (temp_diff.tv_usec < 0) {
        temp_diff.tv_usec += 1000000;
        temp_diff.tv_sec -= 1;
    }
    return 1000000LL * temp_diff.tv_sec + temp_diff.tv_usec;
}

/*
 * Print all keys and values from red-black tree to stdout.
 */
void printAll(RbtHandle rbtHandle) {
    zadbDataKey *zdbkey = NULL;
    zadbDataVal *zdbval = NULL;
    char *table, *key, *field, *val;
    ZADB_DATA_TYPE table_size, key_size, field_size, val_size;
    RbtIterator iterator = rbtBegin(rbtHandle);
    int isStr = 0;
    ZADB_DATA_NUM num = 123456789;
    while (iterator != NULL) {
        rbtKeyValue(rbtHandle, iterator, (void*) &zdbkey, (void*) &zdbval);
        zadbKeyGet(zdbkey, &table, &table_size, &key, &key_size, &field, &field_size);
        zadbValGet(zdbval, &val, &val_size, &num, &isStr);
        if (isStr) {
            printf("%.*s ^ %.*s : %.*s > \"%.*s\" \n", table_size, table, key_size, key, field_size, field, val_size, val);
        } else {
            printf("%.*s ^ %.*s : %.*s > %lld \n", table_size, table, key_size, key, field_size, field, num);
        }
        iterator = rbtNext(rbtHandle, iterator);
    }
}

/*
 * Used for debug. Print lua stack to stdout.
 */
void luaPrintLuaStack(lua_State * L) {
    for (int i = 1; i <= lua_gettop(L); ++i) {
        printf("\tstack[%d] = '%s'\n", i, lua_tostring(L, i));
    }
}

int luaCheckErrors(int status) {
    switch (status) {
    case LUA_ERRSYNTAX:
        fprintf(stderr, " Error: LUA_ERRSYNTAX\n");
        break;
    case LUA_ERRMEM:
        fprintf(stderr, " Error: LUA_ERRMEM\n");
        break;
    default:
        fprintf(stderr, " Error: %d\n", status);
    }
    luaPrintLuaStack(luaState);
    return status;
}

/*
 * Help function for convert to string most of lua types and check requirements.
 *
 * L: lua state or lua thread
 * index: converted variable position in stack
 *
 * size: out string len
 *
 * return string or null
 *
 */
const char * luaToString(lua_State *L, int index, size_t * size) {
    int type = lua_type(L, index);
    const char *out;
    *size = 0;
    switch (type) {
    case LUA_TNUMBER:
        lua_pushvalue(L, index);
        out = lua_tolstring(L, -1, size);
        break;
    case LUA_TSTRING:
        out = lua_tolstring(L, index, size);
        break;
    case LUA_TBOOLEAN:
        if (lua_toboolean(L, index)) {
            out = "True";
            *size = 4;
        } else {
            out = "False";
            *size = 5;
        }
        break;
    default:
        out = NULL;
        *size = 0;
        break;
    }
    if (*size > ZADB_DATA_MAXSIZE) {
        *size = ZADB_DATA_MAXSIZE;
    }
    return out;
}

/*
 * Helper function. Compare two string
 */
int isStringEqual(const char *s1, size_t size1, const char *s2, size_t size2) {
    if(size1 != size2){
        return 0;
    }
    register unsigned char u1, u2;
    while (size1-- > 0) {
        u1 = (unsigned char) *s1++;
        u2 = (unsigned char) *s2++;
        if (u1 != u2)
            return 0;
    }
    return 1;
}


/*
 * Used for debug. Print all keys and values from red-black tree.
 */
int databasePrintAll(lua_State *L) {
    printAll(rbtHandle);
    return 0;
}


/*
 * get or get and delete key-value from red-black tree
 *
 * key contain three section:
 *
 * table string
 * key string
 * field string
 *
 * input on lua stack:
 * 1 - table string
 * 2 - key string
 * 3 - field string
 *
 *
 * L: lua state or lua thread
 * delete: if delete != 0 then  key-value will be deleted
 *
 * put value from red-black tree. String or number
 * return number variables in lua stack
 *
 */
int databaseHGet_(lua_State *L, int delete) {
    if (lua_gettop(L) != 3 || !lua_isstring(L, 1) || !lua_isstring(L, 2) || !lua_isstring(L, 3)) {
        lua_pushnil(L);
        return 1;
    }
    char *table, *key, *field, *val;
    size_t o_table_size, o_key_size, o_field_size;
    ZADB_DATA_TYPE table_size, key_size, field_size, val_size;
    zadbDataKey from, zdbkey;
    zadbDataVal zdbval;
    ZADB_DATA_NUM num;
    int isStr;

    const char * o_table = luaToString(L, 1, &o_table_size);
    const char * o_key = luaToString(L, 2, &o_key_size);
    const char * o_field = luaToString(L, 3, &o_field_size);

    if (o_table == NULL || o_table_size == 0 || o_key == NULL || o_key_size == 0 || o_field == NULL || o_field_size == 0) {
        lua_pushnil(L);
        return 1;
    }

    from = zadbKeyNew(o_table, o_table_size, o_key, o_key_size, o_field, o_field_size, 1);
    RbtIterator iterator = rbtFind(rbtHandle, from);
    if (iterator != NULL) {
        rbtKeyValue(rbtHandle, iterator, (void *) &zdbkey, (void *) &zdbval);
        zadbKeyGet(zdbkey, &table, &table_size, &key, &key_size, &field, &field_size);
        zadbValGet(zdbval, &val, &val_size, &num, &isStr);
        if (isStr) {
            if (val != NULL) {
                lua_pushlstring(L, val, val_size);
            } else {
                lua_pushlstring(L, "", 0);
            }
        } else {
            lua_pushinteger(L, num);
        }
        if (delete) {
            db_stat_del++;
            zadbKeyFree(zdbkey);
            zadbValFree(zdbval);
            rbtErase(rbtHandle, iterator);
        }else{
            db_stat_get++;
        }
    } else {
        lua_pushnil(L);
    }
    zadbKeyFree(from);
    return 1;
}

/*
 * helper function for get key-value from red-black tree
 *
 * L: lua state or lua thread
 *
 */
int databaseHGet(lua_State *L) {
    return databaseHGet_(L, 0);
}

/*
 * helper function for delete key-value from red-black tree
 *
 * L: lua state or lua thread
 *
 */
int databaseHDel(lua_State *L) {
    return databaseHGet_(L, 1);
}


/*
 * get all key-value from red-black tree
 *
 * key contain three section:
 *
 * table string
 * key string
 * field string
 *
 * input on lua stack:
 * 1 - table string
 * 2 - key string
 *
 * L: lua state or lua thread
 *
 * put lua table with field-value to lua stack
 * in case some error returned table will be empty
 * return number variables in lua stack
 */
int databaseHGetall(lua_State *L) {
    if (lua_gettop(L) != 2 || !lua_isstring(L, 1) || !lua_isstring(L, 2)) {
        lua_createtable(L, 0, 0);
        return 1;
    }
    char *table, *key, *field, *val;
    size_t o_table_size, o_key_size;
    ZADB_DATA_TYPE table_size, key_size, field_size, val_size;
    zadbDataKey from, zdbkey;
    zadbDataVal zdbval;
    ZADB_DATA_NUM num;
    int isStr;

    const char * o_table = luaToString(L, 1, &o_table_size);
    const char * o_key = luaToString(L, 2, &o_key_size);

    if (o_table == NULL || o_table_size == 0 || o_key == NULL || o_key_size == 0) {
        lua_createtable(L, 0, 0);
        return 1;
    }
    from = zadbKeyNew(o_table, o_table_size, o_key, o_key_size, NULL, 0, 1);
    lua_createtable(L, 0, 10);
    RbtIterator iterator;
    iterator = rbtScan(rbtHandle, from);
    while (iterator != NULL) {
        rbtKeyValue(rbtHandle, iterator, (void *) &zdbkey, (void *) &zdbval);
        zadbKeyGet(zdbkey, &table, &table_size, &key, &key_size, &field, &field_size);
        if (!isStringEqual(o_table, o_table_size, table, table_size) || !isStringEqual(o_key, o_key_size, key, key_size)) {
            break;
        }
        zadbValGet(zdbval, &val, &val_size, &num, &isStr);
        lua_pushlstring(L, field, field_size);
        if (isStr) {
            if (val != NULL) {
                lua_pushlstring(L, val, val_size);
            } else {
                lua_pushlstring(L, "", 0);
            }
        } else {
            lua_pushinteger(L, num);
        }

        lua_rawset(L, -3);
        iterator = rbtNext(rbtHandle, iterator);
        db_stat_get++;
    }
    zadbKeyFree(from);
    return 1;
}

/*
 * Delete all key-value from red-black tree
 *
 * key contain three section:
 *
 * table string
 * key string
 * field string
 *
 * input on lua stack:
 * 1 - table string
 * 2 - key string
 *
 * L: lua state or lua thread
 *
 * put on lua stack empty lua table //TODO what for?
 * return number variables in lua stack
 */
int databaseHDelall(lua_State *L) {
    if (lua_gettop(L) != 2 || !lua_isstring(L, 1) || !lua_isstring(L, 2)) {
        lua_createtable(L, 0, 0);
        return 1;
    }

    char * table, *key, *field;
    size_t o_table_size, o_key_size;
    ZADB_DATA_TYPE table_size, key_size, field_size;
    zadbDataKey from, zdbkey;
    zadbDataVal zdbval;

    const char * o_table = luaToString(L, 1, &o_table_size);
    const char * o_key = luaToString(L, 2, &o_key_size);

    if (o_table == NULL || o_table_size == 0 || o_key == NULL || o_key_size == 0) {
        lua_createtable(L, 0, 0);
        return 1;
    }
    from = zadbKeyNew(o_table, o_table_size, o_key, o_key_size, NULL, 0, 1);
    lua_createtable(L, 0, 0);
    RbtIterator iterator;
    iterator = rbtScan(rbtHandle, from);
    while (iterator != NULL) {
        rbtKeyValue(rbtHandle, iterator, (void *) &zdbkey, (void *) &zdbval);
        zadbKeyGet(zdbkey, &table, &table_size, &key, &key_size, &field, &field_size);
        if (!isStringEqual(o_table, o_table_size, table, table_size) || !isStringEqual(o_key, o_key_size, key, key_size)) {
            break;
        }
        zadbKeyFree(zdbkey);
        zadbValFree(zdbval);
        rbtErase(rbtHandle, iterator);
        db_stat_del++;
        iterator = rbtScan(rbtHandle, from);
    }
    zadbKeyFree(from);
    return 1;
}

/*
 * Insert key-value to red-black tree
 *
 * key contain three section:
 *
 * table string
 * key string
 * field string
 *
 * input on lua stack:
 * 1 - table string
 * 2 - key string
 * 3 - table that contain field-value
 *
 * L: lua state or lua thread
 *
 * no returns variables on lua stack
 * return always 0
 */
int databaseHSet(lua_State *L) {
    if (lua_gettop(L) != 3 || !lua_istable(L, 3) || !lua_isstring(L, 1) || !lua_isstring(L, 2)) {
        return 0;
    }
    size_t o_table_size, o_key_size, field_size, val_size;
    zadbDataKey zdbkey;
    zadbDataVal zdbval, rbdup;
    lua_Integer num;

    const char * o_table = luaToString(L, 1, &o_table_size);
    const char * o_key = luaToString(L, 2, &o_key_size);

    if (o_table == NULL || o_table_size == 0 || o_key == NULL || o_key_size == 0) {
        return 0;
    }

    lua_pushnil(L);
    while (lua_next(L, 3) != 0) {
        const char * field = luaToString(L, -2, &field_size);
        int type = lua_type(L, -1);
        if (type != LUA_TNUMBER) {
            const char * val = luaToString(L, -1, &val_size);

            zdbval = zadbValNewStr(val, val_size);
        } else {
            num = lua_tointeger(L, -1);
            zdbval = zadbValNewInt(num);
        }

        zdbkey = zadbKeyNew(o_table, o_table_size, o_key, o_key_size, field, field_size, 0);

        RbtStatus status = rbtInsert(rbtHandle, zdbkey, zdbval, &rbdup);
        switch (status) {
        case RBT_STATUS_DUPLICATE_KEY:
            zadbKeyFree(zdbkey);
            zadbValFree(rbdup);
            db_stat_upd++;
            break;
        case RBT_STATUS_OK:
            db_stat_set++;
            break;
        default:
            perror("error databaseHSet");
        }
        lua_pop(L, 1);
    }
    return 0;
}


/*
 * get from lua stack string and send with socket
 *
 * L: lua state or lua thread
 *
 * socket: socket
 *
 */
int processLuaResult(lua_State *L, int socket) {
    if (lua_gettop(L) != 1) {
        return 0;
    }
    if (lua_isstring(L, 1)) {
        size_t str_size;
        char *str = (char *) lua_tolstring(L, 1, &str_size);
        if (str != NULL && str_size > 0) {
            return send(socket, str, str_size, MSG_NOSIGNAL);
        }
        return 0;
    }
    printf("Lua script returns wrong data\n");
    return 0;
}


/*
 * run lua thread
 *
 * socket: socket
 */
int processRequest(int socket) {
    int nres = 0;
    int rc = lua_resume(luaStateThread, NULL, 2, &nres);
    switch (rc) {
    case LUA_YIELD:
        if (nres > 0) {
            processLuaResult(luaStateThread, socket);
            lua_settop(luaStateThread, 0);
        }
        break;
    case 0:
        printf("--- coroutine finished normally ---\n");
        exit(1);
    default:
        printf("!!! coroutine finished for error !!!\n");
        luaPrintLuaStack(luaStateThread);
        exit(1);
    }
    return 0;
}

#define SOCKET_LOOP_ERR 1
#define SOCKET_LOOP_OK 0
#define SOCKET_LOOP_MAX_CONNECTIONS 100
#define SOCKET_LOOP_MASTER 0


/*
 * init socket and set options
 *
 * input port
 *
 * return socket
 */
int socketInit(int* outSocket, int port) {
    if ((*outSocket = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        perror("socket failed");
        return SOCKET_LOOP_ERR;
    }

    int opt = 1;
    if (setsockopt(*outSocket, SOL_SOCKET, SO_REUSEADDR, (char *) &opt, sizeof(opt)) < 0) {
        perror("setsockopt failed");
        return SOCKET_LOOP_ERR;
    }

    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);

    if (bind(*outSocket, (struct sockaddr *) &address, sizeof(address)) < 0) {
        perror("bind failed");
        return SOCKET_LOOP_ERR;
    }
    if (listen(*outSocket, SOCKET_LOOP_MAX_CONNECTIONS) < 0) {
        perror("listen failed");
        return SOCKET_LOOP_ERR;
    }
    printf("Listener on port %d \n", port);
    printf("Waiting for connections ...\n");
    return 0;
}

#define PROTOCOL_OK 0
#define PROTOCOL_ERR 1
#define PROTOCOL_EMPTY 2
#define RESP_ARRAY '*'
#define RESP_BULKSTRING '$'
#define RESP_INTEGER ':'

/*
 * decode Redis serialization protocol (RESP) string to lua objects and put on stack
 *
 *
 * L: lua state or lua thread
 * buf: start of buffer string
 * end: end of buffer string
 *
 * return status: *
 * PROTOCOL_OK - 0
 * PROTOCOL_ERR - 1
 * PROTOCOL_EMPTY - 2
 *
 */
int parseRespToLua(lua_State *L, char * buf, char * end) {
    int luatop = lua_gettop(L);
    if (*buf != RESP_ARRAY) {
        return PROTOCOL_ERR;
    }
    buf++;
    int resp_array_size = 0;
    while (*buf != '\r') {
        resp_array_size = (resp_array_size * 10) + (*buf - '0');
        buf++;
        if (buf > end) {
            return PROTOCOL_ERR;
        }
    }

    if (resp_array_size < 1) {
        return PROTOCOL_ERR;
    }
    buf += 2;
    int i = 0;
    for (i = 0; i < resp_array_size; i++) {
        if (*buf == RESP_BULKSTRING) {
            buf++;
            int str_size = 0;
            while (*buf != '\r') {
                str_size = (str_size * 10) + (*buf - '0');
                buf++;
                if (buf > end) {
                    lua_settop(luaStateThread, luatop);
                    return PROTOCOL_ERR;
                }
            }
            buf += 2;

            char * endstr = buf + str_size;
            if (endstr + 2 > end) {
                lua_settop(luaStateThread, luatop);
                return PROTOCOL_ERR;
            }
            if (i == 0) {
                lua_pushlstring(luaStateThread, buf, str_size);
                lua_createtable(luaStateThread, resp_array_size, 0);
            } else {
                lua_pushlstring(luaStateThread, buf, str_size);
                if (!(i % 2)) {
                    lua_rawset(luaStateThread, -3);
                }
            }
            buf = endstr + 2;
        } else if (*buf == RESP_INTEGER) {
            buf++;
            lua_Integer num = 0;
            while (*buf != '\r') {
                num = (num * 10) + (*buf - '0');
                buf++;
                if (buf > end) {
                    lua_settop(L, luatop);
                    return PROTOCOL_ERR;
                }
            }

            buf += 2;
            if (i == 0) {
                lua_pushinteger(L, num);
                lua_createtable(luaStateThread, resp_array_size, 0);
            } else {
                lua_pushinteger(L, num);
                if (!(i % 2)) {
                    lua_rawset(luaStateThread, -3);
                }
            }

        } else {
            lua_settop(L, luatop);
            return PROTOCOL_ERR;
        }

    }
    if (!(i % 2)) {
        lua_pushlstring(L, "", 0);
        lua_rawset(L, -3);
    }
    return PROTOCOL_OK;
}


/*
 * helper function
 *
 * put to lua stack input
 *
 * 1 - cmd: some string
 * 2 - host: host string
 * 3 - port: port
 *
 */
void internalEventToLua(lua_State *L, char * cmd, char * host, int port) {
    lua_pushstring(luaStateThread, cmd);
    lua_createtable(luaStateThread, 0, 3);
    lua_pushstring(luaStateThread, "name");
    lua_pushstring(luaStateThread, host);
    lua_rawset(luaStateThread, -3);
    lua_pushstring(luaStateThread, "port");
    lua_pushinteger(luaStateThread, port);
    lua_rawset(luaStateThread, -3);
}

#define MASTER_SOCKET_IDX 0
#define MAINLOOP_START_IDX 1


//for debug malloc-free counter
extern long long  malloccounter;


/*
 * main function for read data from socket and run lua thread
 *
 * input socket tcp port
 *
 */
int mainLoop(int port) {
    int requests = 0;
    char buf[SOCKET_CLIENT_BUFFER];
    struct sockaddr_in address;
    int addrlen = sizeof(address);
    const int nfds = SOCKET_LOOP_MAX_CONNECTIONS + 1;
    struct pollfd pfds[nfds];
    for (int i = 0; i < nfds; i++) {
        pfds[i].fd = -1;
        pfds[i].events = POLLIN;
    }

    if (socketInit(&pfds[MASTER_SOCKET_IDX].fd, port)) {
        fprintf(stderr, "za_socket_init failed\n");
    }
    gettimeofday(&g_starttime, 0);
    int timeout = 1000;
    while (1) {
        int ready = poll(pfds, nfds, timeout);
        if ((ready < 0) && (errno != EINTR)) {
            perror("listen failed");
            return SOCKET_LOOP_ERR;
        }
        if (pfds[MASTER_SOCKET_IDX].revents & POLLIN) {
            int new_socket = accept(pfds[MASTER_SOCKET_IDX].fd, (struct sockaddr *) &address, (socklen_t*) &addrlen);
            if (new_socket < 0) {
                perror("accept failed");
                return SOCKET_LOOP_ERR;
            }
            for (int i = MAINLOOP_START_IDX; i < nfds; i++) {
                if (pfds[i].fd < 0) {
                    pfds[i].fd = new_socket;
                    internalEventToLua(luaStateThread, "CONNECT", inet_ntoa(address.sin_addr), ntohs(address.sin_port));
                    processRequest(new_socket);
                    break;
                }
            }
        }

        for (int i = MAINLOOP_START_IDX; i < nfds; i++) {
            if (pfds[i].revents) {
                int nread = read(pfds[i].fd, buf, SOCKET_CLIENT_BUFFER);
                if (nread < 1) {
                    if (errno != 0) {
                        perror("read failed");
                    }
                    internalEventToLua(luaStateThread, "DISCONNECT", inet_ntoa(address.sin_addr), ntohs(address.sin_port));
                    processRequest(pfds[i].fd);
                    close(pfds[i].fd);
                    pfds[i].fd = -1;
                } else {
                    int rc = parseRespToLua(luaStateThread, buf, buf + nread);
                    if (rc == PROTOCOL_ERR) {
                        printf("Wrong protocol. Host disconnected , ip %s , port %d \n", inet_ntoa(address.sin_addr), ntohs(address.sin_port));
                        internalEventToLua(luaStateThread, "DISCONNECT", inet_ntoa(address.sin_addr), ntohs(address.sin_port));
                        processRequest(pfds[i].fd);
                        close(pfds[i].fd);
                        pfds[i].fd = -1;
                    } else {
                        requests++;
                        processRequest(pfds[i].fd);
                    }
                }
            }
        }
        gettimeofday(&g_endtime, 0);
        timediff = timeval_diff(&g_endtime, &g_starttime);
        if (timediff > 1000000) {
            fprintf(stderr, "Req_sec=%8d mem_alloc=%8lld db_get_sec=%8lld db_set_sec=%8lld db_del_sec=%8lld db_upd_sec=%8lld\n", requests, malloccounter, db_stat_get, db_stat_set, db_stat_del, db_stat_upd);
            gettimeofday(&g_starttime, 0);
            requests = 0;
            db_stat_get = 0;
            db_stat_set = 0;
            db_stat_del = 0;
            db_stat_upd = 0;
            timeout = 1000;
        } else {
            timeout = timeout - timediff / 1000;
            if (timeout < 0) {
                timeout = 0;
            }
        }
    }
}

/*
 * init lua thread
 *
 * create lus thread
 * add lua function for work with red-black tree
 *
 */
int initLua() {
    luaState = luaL_newstate();
    luaL_openlibs(luaState);
    lua_newtable(luaState);
    lua_pushcfunction(luaState, databaseHGet);
    lua_setfield(luaState, -2, "hget");
    lua_pushcfunction(luaState, databaseHGetall);
    lua_setfield(luaState, -2, "hgetall");
    lua_pushcfunction(luaState, databaseHDel);
    lua_setfield(luaState, -2, "hdel");
    lua_pushcfunction(luaState, databaseHDelall);
    lua_setfield(luaState, -2, "hdelall");
    lua_pushcfunction(luaState, databaseHSet);
    lua_setfield(luaState, -2, "hset");
    lua_pushcfunction(luaState, databasePrintAll);
    lua_setfield(luaState, -2, "printall");
    lua_setglobal(luaState, "za_db");
    int status = luaL_loadfile(luaState, "main.lua");
    if (status != LUA_OK) {
        fprintf(stderr, "Can't load file main.lua ");
        luaCheckErrors(status);
        return 1;
    }
    lua_setfield(luaState, LUA_REGISTRYINDEX, "za_code");
    luaStateThread = lua_newthread(luaState);
    lua_getfield(luaStateThread, LUA_REGISTRYINDEX, "za_code");
    status = lua_pcall(luaStateThread, 0, 1, 0);
    if (status != LUA_OK) {
        fprintf(stderr, "lua_pcall failed. ");
        luaCheckErrors(status);
        return 1;
    }

    return 0;
}


int main(int argc, char **argv) {
    int port = DEFAULT_PORT;
    for (int i = 1; i < argc - 1; i++) {
        if (!strcmp(argv[i], "-port")) {
            port = atoi(argv[i + 1]);
            break;
        }
    }

    rbtHandle = rbtNew(&zadbKeyFieldCompare);
    if (rbtHandle == NULL) {
        perror("rbtNew failed\n");
        return 1;
    }
    if (initLua()) {
        return 1;
    }
    mainLoop(port);
    return 0;
}
