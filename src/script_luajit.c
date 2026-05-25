/*
 * Copyright (c) 2009-2021, Redis Ltd.
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * Core shared scripting logic for the LuaJIT engine module.
 * This is a close adaptation of the built-in Lua engine's script_lua.c
 * but using LuaJIT instead.
 */

#include "fpconv_dtoa.h"
#include "valkeymodule.h"
#include "script_luajit.h"
#include "engine_structs.h"
#include "sha1.h"
#include "rand.h"

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <stdio.h>
#include <errno.h>
#include <time.h>
#include <stdarg.h>
#include <limits.h>

#define PROPAGATE_NONE 0
#define PROPAGATE_AOF 1
#define PROPAGATE_REPL 2

#define LL_DEBUG 0
#define LL_VERBOSE 1
#define LL_NOTICE 2
#define LL_WARNING 3

typedef struct luajitFuncCallCtx {
    ValkeyModuleCtx *module_ctx;
    ValkeyModuleScriptingEngineServerRuntimeCtx *run_ctx;
    ValkeyModuleScriptingEngineSubsystemType type;
    int replication_flags;
    int resp;
    int lua_enable_insecure_api;
} luajitFuncCallCtx;

static void _serverPanic(const char *file, int line, const char *msg, ...) {
    fprintf(stderr, "------------------------------------------------");
    fprintf(stderr, "!!! Software Failure. Press left mouse button to continue");
    fprintf(stderr, "Guru Meditation: %s #%s:%d", msg, file, line);
    abort();
}

#define serverPanic(...) _serverPanic(__FILE__, __LINE__, __VA_ARGS__)

typedef uint64_t monotime;

/* clock_gettime() is specified in POSIX.1b (1993).  Even so, some systems
 * did not support this until much later.  CLOCK_MONOTONIC is technically
 * optional and may not be supported - but it appears to be universal.
 * If this is not supported, provide a system-specific alternate version.  */
monotime getMonotonicUs(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ((uint64_t)ts.tv_sec) * 1000000 + ts.tv_nsec / 1000;
}

inline uint64_t elapsedUs(monotime start_time) {
    return getMonotonicUs() - start_time;
}

inline uint64_t elapsedMs(monotime start_time) {
    return elapsedUs(start_time) / 1000;
}

static int server_math_random(lua_State *L);
static int server_math_randomseed(lua_State *L);

static void luajitReplyToServerReply(ValkeyModuleCtx *ctx, int resp_version, lua_State *lua);

/*
 * Save the give pointer on Lua registry, used to save the Lua context and
 * function context so we can retrieve them from lua_State.
 */
void luajitSaveOnRegistry(lua_State *lua, const char *name, void *ptr) {
    lua_pushstring(lua, name);
    if (ptr) {
        lua_pushlightuserdata(lua, ptr);
    } else {
        lua_pushnil(lua);
    }
    lua_settable(lua, LUA_REGISTRYINDEX);
}

/*
 * Get a saved pointer from registry
 */
void *luajitGetFromRegistry(lua_State *lua, const char *name) {
    lua_pushstring(lua, name);
    lua_gettable(lua, LUA_REGISTRYINDEX);

    if (lua_isnil(lua, -1)) {
        lua_pop(lua, 1); /* pops the value */
        return NULL;
    }
    ValkeyModule_Assert(lua_islightuserdata(lua, -1)); /* must be light user data */

    void *ptr = (void *)lua_topointer(lua, -1);
    ValkeyModule_Assert(ptr);

    lua_pop(lua, 1); /* pops the value */

    return ptr;
}

char *ljm_asprintf(char const *fmt, ...) {
    va_list args;

    va_start(args, fmt);
    size_t str_len = vsnprintf(NULL, 0, fmt, args) + 1;
    va_end(args);

    char *str = ValkeyModule_Alloc(str_len);

    va_start(args, fmt);
    vsnprintf(str, str_len, fmt, args);
    va_end(args);

    return str;
}

char *ljm_strcpy(const char *str) {
    size_t len = strlen(str);
    char *res = ValkeyModule_Alloc(len + 1);
    memcpy(res, str, len + 1);
    return res;
}

char *ljm_strtrim(char *s, const char *cset) {
    char *end, *sp, *ep;
    size_t len;

    sp = s;
    ep = end = s + strlen(s) - 1;
    while (sp <= end && strchr(cset, *sp)) sp++;
    while (ep > sp && strchr(cset, *ep)) ep--;
    len = (ep - sp) + 1;
    if (s != sp) memmove(s, sp, len);
    s[len] = '\0';
    return s;
}

/* This function is used in order to push an error on the Lua stack in the
 * format used by luajitPcall to return errors, which is a lua table
 * with an "err" field set to the error string including the error code.
 * Note that this table is never a valid reply by proper commands,
 * since the returned tables are otherwise always indexed by integers, never by strings.
 *
 * The function takes ownership on the given err_buffer. */
static void luajitPushErrorBuff(lua_State *lua, const char *err_buffer) {
    char *msg;

    char *final_msg = NULL;
    /* There are two possible formats for the received `error` string:
     * 1) "-CODE msg": we remove the leading '-' since we don't store it as part of the lua error format.
     * 2) "msg":       we prepend a generic 'ERR' code since all error statuses need some error code.
     * We support format (1) so this function can reuse the error messages used in other places.
     * We support format (2) so it'll be easy to pass descriptive errors to this function without worrying about format.
     * Callers must not embed an error code in the message — pass "-CODE msg" if they want a specific code.
     */
    if (err_buffer[0] == '-') {
        /* derive error code from the message */
        char *err_msg = strstr(err_buffer, " ");
        if (!err_msg) {
            msg = ljm_strcpy(err_buffer + 1);
            final_msg = ljm_asprintf("ERR %s", msg);
        } else {
            char *err_copy = ljm_strcpy(err_buffer);
            char *space = strstr(err_copy, " ");
            *space = '\0';
            msg = ljm_strcpy(space + 1);
            msg = ljm_strtrim(msg, "\r\n");
            final_msg = ljm_asprintf("%s %s", err_copy + 1, msg);
            ValkeyModule_Free(err_copy);
        }
    } else {
        msg = ljm_strcpy(err_buffer);
        msg = ljm_strtrim(msg, "\r\n");
        final_msg = ljm_asprintf("ERR %s", msg);
    }
    /* Trim newline at end of string. If we reuse the ready-made error objects (case 1 above) then we might
     * have a newline that needs to be trimmed. In any case the lua server error table shouldn't end with a newline. */

    lua_newtable(lua);
    lua_pushstring(lua, "err");
    lua_pushstring(lua, final_msg);
    lua_settable(lua, -3);

    ValkeyModule_Free(msg);
    ValkeyModule_Free(final_msg);
}

void luajitPushError(lua_State *lua, const char *error) {
    luajitPushErrorBuff(lua, error);
}

/* In case the error set into the Lua stack by luajitPushError() was generated
 * by the non-error-trapping version of server.pcall(), which is server.call(),
 * this function will raise the Lua error so that the execution of the
 * script will be halted. */
int luajitError(lua_State *lua) {
    return lua_error(lua);
}

/* ---------------------------------------------------------------------------
 * Server reply to Lua type conversion functions.
 * ------------------------------------------------------------------------- */

static void callReplyToLuaType(lua_State *lua, ValkeyModuleCallReply *reply, int resp) {
    int type = ValkeyModule_CallReplyType(reply);
    switch (type) {
    case VALKEYMODULE_REPLY_STRING: {
        if (!lua_checkstack(lua, 1)) {
            /* Increase the Lua stack if needed, to make sure there is enough room
             * to push elements to the stack. On failure, exit with panic. */
            serverPanic("lua stack limit reach when parsing server.call reply");
        }
        size_t len = 0;
        const char *str = ValkeyModule_CallReplyStringPtr(reply, &len);
        lua_pushlstring(lua, str, len);
        break;
    }
    case VALKEYMODULE_REPLY_SIMPLE_STRING: {
        if (!lua_checkstack(lua, 3)) {
            /* Increase the Lua stack if needed, to make sure there is enough room
             * to push elements to the stack. On failure, exit with panic. */
            serverPanic("lua stack limit reach when parsing server.call reply");
        }
        size_t len = 0;
        const char *str = ValkeyModule_CallReplyStringPtr(reply, &len);
        lua_newtable(lua);
        lua_pushstring(lua, "ok");
        lua_pushlstring(lua, str, len);
        lua_settable(lua, -3);
        break;
    }
    case VALKEYMODULE_REPLY_INTEGER: {
        if (!lua_checkstack(lua, 1)) {
            /* Increase the Lua stack if needed, to make sure there is enough room
             * to push elements to the stack. On failure, exit with panic. */
            serverPanic("lua stack limit reach when parsing server.call reply");
        }
        long long val = ValkeyModule_CallReplyInteger(reply);
        lua_pushnumber(lua, (lua_Number)val);
        break;
    }
    case VALKEYMODULE_REPLY_ARRAY: {
        if (!lua_checkstack(lua, 2)) {
            /* Increase the Lua stack if needed, to make sure there is enough room
             * to push elements to the stack. On failure, exit with panic. */
            serverPanic("lua stack limit reach when parsing server.call reply");
        }
        size_t items = ValkeyModule_CallReplyLength(reply);
        lua_createtable(lua, items, 0);

        for (size_t i = 0; i < items; i++) {
            ValkeyModuleCallReply *val = ValkeyModule_CallReplyArrayElement(reply, i);

            lua_pushnumber(lua, i + 1);
            callReplyToLuaType(lua, val, resp);
            lua_settable(lua, -3);
        }
        break;
    }
    case VALKEYMODULE_REPLY_NULL:
    case VALKEYMODULE_REPLY_ARRAY_NULL:
        if (!lua_checkstack(lua, 1)) {
            /* Increase the Lua stack if needed, to make sure there is enough room
             * to push elements to the stack. On failure, exit with panic. */
            serverPanic("lua stack limit reach when parsing server.call reply");
        }
        if (resp == 2) {
            lua_pushboolean(lua, 0);
        } else {
            lua_pushnil(lua);
        }
        break;
    case VALKEYMODULE_REPLY_MAP: {
        if (!lua_checkstack(lua, 3)) {
            /* Increase the Lua stack if needed, to make sure there is enough room
             * to push elements to the stack. On failure, exit with panic. */
            serverPanic("lua stack limit reach when parsing server.call reply");
        }

        size_t items = ValkeyModule_CallReplyLength(reply);
        lua_newtable(lua);
        lua_pushstring(lua, "map");
        lua_createtable(lua, 0, items);

        for (size_t i = 0; i < items; i++) {
            ValkeyModuleCallReply *key = NULL;
            ValkeyModuleCallReply *val = NULL;
            ValkeyModule_CallReplyMapElement(reply, i, &key, &val);

            callReplyToLuaType(lua, key, resp);
            callReplyToLuaType(lua, val, resp);
            lua_settable(lua, -3);
        }
        lua_settable(lua, -3);
        break;
    }
    case VALKEYMODULE_REPLY_SET: {
        if (!lua_checkstack(lua, 3)) {
            /* Increase the Lua stack if needed, to make sure there is enough room
             * to push elements to the stack. On failure, exit with panic. */
            serverPanic("lua stack limit reach when parsing server.call reply");
        }

        size_t items = ValkeyModule_CallReplyLength(reply);
        lua_newtable(lua);
        lua_pushstring(lua, "set");
        lua_createtable(lua, 0, items);

        for (size_t i = 0; i < items; i++) {
            ValkeyModuleCallReply *val = ValkeyModule_CallReplySetElement(reply, i);

            callReplyToLuaType(lua, val, resp);
            lua_pushboolean(lua, 1);
            lua_settable(lua, -3);
        }
        lua_settable(lua, -3);
        break;
    }
    case VALKEYMODULE_REPLY_BOOL: {
        if (!lua_checkstack(lua, 1)) {
            /* Increase the Lua stack if needed, to make sure there is enough room
             * to push elements to the stack. On failure, exit with panic. */
            serverPanic("lua stack limit reach when parsing server.call reply");
        }
        int b = ValkeyModule_CallReplyBool(reply);
        lua_pushboolean(lua, b);
        break;
    }
    case VALKEYMODULE_REPLY_DOUBLE: {
        if (!lua_checkstack(lua, 3)) {
            /* Increase the Lua stack if needed, to make sure there is enough room
             * to push elements to the stack. On failure, exit with panic. */
            serverPanic("lua stack limit reach when parsing server.call reply");
        }
        double d = ValkeyModule_CallReplyDouble(reply);
        lua_newtable(lua);
        lua_pushstring(lua, "double");
        lua_pushnumber(lua, d);
        lua_settable(lua, -3);
        break;
    }
    case VALKEYMODULE_REPLY_BIG_NUMBER: {
        if (!lua_checkstack(lua, 3)) {
            /* Increase the Lua stack if needed, to make sure there is enough room
             * to push elements to the stack. On failure, exit with panic. */
            serverPanic("lua stack limit reach when parsing server.call reply");
        }
        size_t len = 0;
        const char *str = ValkeyModule_CallReplyBigNumber(reply, &len);
        lua_newtable(lua);
        lua_pushstring(lua, "big_number");
        lua_pushlstring(lua, str, len);
        lua_settable(lua, -3);
        break;
    }
    case VALKEYMODULE_REPLY_VERBATIM_STRING: {
        if (!lua_checkstack(lua, 5)) {
            /* Increase the Lua stack if needed, to make sure there is enough room
             * to push elements to the stack. On failure, exit with panic. */
            serverPanic("lua stack limit reach when parsing server.call reply");
        }
        size_t len = 0;
        const char *format = NULL;
        const char *str = ValkeyModule_CallReplyVerbatim(reply, &len, &format);
        lua_newtable(lua);
        lua_pushstring(lua, "verbatim_string");
        lua_newtable(lua);
        lua_pushstring(lua, "string");
        lua_pushlstring(lua, str, len);
        lua_settable(lua, -3);
        lua_pushstring(lua, "format");
        lua_pushlstring(lua, format, 3);
        lua_settable(lua, -3);
        lua_settable(lua, -3);
        break;
    }
    case VALKEYMODULE_REPLY_ERROR: {
        if (!lua_checkstack(lua, 3)) {
            /* Increase the Lua stack if needed, to make sure there is enough room
             * to push elements to the stack. On failure, exit with panic. */
            serverPanic("lua stack limit reach when parsing server.call reply");
        }
        const char *err = ValkeyModule_CallReplyStringPtr(reply, NULL);
        /* The reply parser strips the leading '-' from the RESP error, so
         * `err` is of the form "CODE rest" (e.g. "WRONGTYPE Operation..."
         * or "ERR DB index is out of range"). Re-add the '-' so
         * luajitPushErrorBuff() goes through its "-CODE msg" branch and
         * preserves the code instead of double-prefixing with "ERR ". */
        char *err_with_dash = ljm_asprintf("-%s", err);
        luajitPushErrorBuff(lua, err_with_dash);
        ValkeyModule_Free(err_with_dash);
        /* push a field indicate to ignore updating the stats on this error
         * because it was already updated when executing the command. */
        lua_pushstring(lua, "ignore_error_stats_update");
        lua_pushboolean(lua, 1);
        lua_settable(lua, -3);
        break;
    }
    case VALKEYMODULE_REPLY_ATTRIBUTE: {
        break;
    }
    case VALKEYMODULE_REPLY_PROMISE:
    case VALKEYMODULE_REPLY_UNKNOWN:
    default:
        ValkeyModule_Assert(0);
    }
}

/* ---------------------------------------------------------------------------
 * Lua reply to server reply conversion functions.
 * ------------------------------------------------------------------------- */

static char *strmapchars(char *s, const char *from, const char *to, size_t setlen) {
    size_t j, i, l = strlen(s);

    for (j = 0; j < l; j++) {
        for (i = 0; i < setlen; i++) {
            if (s[j] == from[i]) {
                s[j] = to[i];
                break;
            }
        }
    }
    return s;
}

static char *copy_string_from_lua_stack(lua_State *lua) {
    size_t len;
    const char *str = lua_tolstring(lua, -1, &len);

    if (!str || len == 0) {
        char *res = ValkeyModule_Alloc(1);
        res[0] = '\0';
        return res;
    }

    char *res = ValkeyModule_Alloc(len + 1);
    memcpy(res, str, len);
    res[len] = 0;
    return res;
}

/* Reply to client 'c' converting the top element in the Lua stack to a
 * server reply. As a side effect the element is consumed from the stack.  */
static void luajitReplyToServerReply(ValkeyModuleCtx *ctx, int resp_version, lua_State *lua) {
    int t = lua_type(lua, -1);

    if (!lua_checkstack(lua, 4)) {
        /* Increase the Lua stack if needed to make sure there is enough room
         * to push 4 elements to the stack. On failure, return error.
         * Notice that we need, in the worst case, 4 elements because returning a map might
         * require push 4 elements to the Lua stack.*/
        ValkeyModule_ReplyWithError(ctx, "ERR reached lua stack limit");
        lua_pop(lua, 1); /* pop the element from the stack */
        return;
    }

    switch (t) {
    case LUA_TSTRING:
        ValkeyModule_ReplyWithStringBuffer(ctx, lua_tostring(lua, -1), lua_objlen(lua, -1));
        break;
    case LUA_TBOOLEAN:
        if (resp_version == 2) {
            int b = lua_toboolean(lua, -1);
            if (b) {
                ValkeyModule_ReplyWithLongLong(ctx, 1);
            } else {
                ValkeyModule_ReplyWithNull(ctx);
            }
        } else {
            ValkeyModule_ReplyWithBool(ctx, lua_toboolean(lua, -1));
        }
        break;
    case LUA_TNUMBER: ValkeyModule_ReplyWithLongLong(ctx, (long long)lua_tonumber(lua, -1)); break;
    case LUA_TTABLE:
        /* We need to check if it is an array, an error, or a status reply.
         * Error are returned as a single element table with 'err' field.
         * Status replies are returned as single element table with 'ok'
         * field. */

        /* Handle error reply. */
        /* we took care of the stack size on function start */
        lua_pushstring(lua, "err");
        lua_rawget(lua, -2);
        t = lua_type(lua, -1);
        if (t == LUA_TSTRING) {
            lua_pop(lua,
                    1); /* pop the error message, we will use luajitExtractErrorInformation to get error information */
            errorInfo err_info = {0};
            luajitExtractErrorInformation(lua, &err_info);
            ValkeyModule_ReplyWithCustomErrorFormat(ctx, !err_info.ignore_err_stats_update, "%s", err_info.msg);
            luajitErrorInformationDiscard(&err_info);
            lua_pop(lua, 1); /* pop the result table */
            return;
        }
        lua_pop(lua, 1); /* Discard field name pushed before. */

        /* Handle status reply. */
        lua_pushstring(lua, "ok");
        lua_rawget(lua, -2);
        t = lua_type(lua, -1);
        if (t == LUA_TSTRING) {
            char *ok = copy_string_from_lua_stack(lua);
            strmapchars(ok, "\r\n", "  ", 2);
            ValkeyModule_ReplyWithSimpleString(ctx, ok);
            ValkeyModule_Free(ok);
            lua_pop(lua, 2);
            return;
        }
        lua_pop(lua, 1); /* Discard field name pushed before. */

        /* Handle double reply. */
        lua_pushstring(lua, "double");
        lua_rawget(lua, -2);
        t = lua_type(lua, -1);
        if (t == LUA_TNUMBER) {
            ValkeyModule_ReplyWithDouble(ctx, lua_tonumber(lua, -1));
            lua_pop(lua, 2);
            return;
        }
        lua_pop(lua, 1); /* Discard field name pushed before. */

        /* Handle big number reply. */
        lua_pushstring(lua, "big_number");
        lua_rawget(lua, -2);
        t = lua_type(lua, -1);
        if (t == LUA_TSTRING) {
            char *big_num = copy_string_from_lua_stack(lua);
            strmapchars(big_num, "\r\n", "  ", 2);
            ValkeyModule_ReplyWithBigNumber(ctx, big_num, strlen(big_num));
            ValkeyModule_Free(big_num);
            lua_pop(lua, 2);
            return;
        }
        lua_pop(lua, 1); /* Discard field name pushed before. */

        /* Handle verbatim reply. */
        lua_pushstring(lua, "verbatim_string");
        lua_rawget(lua, -2);
        t = lua_type(lua, -1);
        if (t == LUA_TTABLE) {
            lua_pushstring(lua, "format");
            lua_rawget(lua, -2);
            t = lua_type(lua, -1);
            if (t == LUA_TSTRING) {
                char *format = (char *)lua_tostring(lua, -1);
                lua_pushstring(lua, "string");
                lua_rawget(lua, -3);
                t = lua_type(lua, -1);
                if (t == LUA_TSTRING) {
                    size_t len;
                    char *str = (char *)lua_tolstring(lua, -1, &len);
                    ValkeyModule_ReplyWithVerbatimStringType(ctx, str, len, format);
                    lua_pop(lua, 4);
                    return;
                }
                lua_pop(lua, 1);
            }
            lua_pop(lua, 1);
        }
        lua_pop(lua, 1); /* Discard field name pushed before. */

        /* Handle map reply. */
        lua_pushstring(lua, "map");
        lua_rawget(lua, -2);
        t = lua_type(lua, -1);
        if (t == LUA_TTABLE) {
            int maplen = 0;
            ValkeyModule_ReplyWithMap(ctx, VALKEYMODULE_POSTPONED_LEN);
            /* we took care of the stack size on function start */
            lua_pushnil(lua); /* Use nil to start iteration. */
            while (lua_next(lua, -2)) {
                /* Stack now: table, key, value */
                lua_pushvalue(lua, -2);                           /* Dup key before consuming. */
                luajitReplyToServerReply(ctx, resp_version, lua); /* Return key. */
                luajitReplyToServerReply(ctx, resp_version, lua); /* Return value. */
                /* Stack now: table, key. */
                maplen++;
            }
            ValkeyModule_ReplySetMapLength(ctx, maplen);
            lua_pop(lua, 2);
            return;
        }
        lua_pop(lua, 1); /* Discard field name pushed before. */

        /* Handle set reply. */
        lua_pushstring(lua, "set");
        lua_rawget(lua, -2);
        t = lua_type(lua, -1);
        if (t == LUA_TTABLE) {
            int setlen = 0;
            ValkeyModule_ReplyWithSet(ctx, VALKEYMODULE_POSTPONED_LEN);
            /* we took care of the stack size on function start */
            lua_pushnil(lua); /* Use nil to start iteration. */
            while (lua_next(lua, -2)) {
                /* Stack now: table, key, true */
                lua_pop(lua, 1);                                  /* Discard the boolean value. */
                lua_pushvalue(lua, -1);                           /* Dup key before consuming. */
                luajitReplyToServerReply(ctx, resp_version, lua); /* Return key. */
                /* Stack now: table, key. */
                setlen++;
            }
            ValkeyModule_ReplySetSetLength(ctx, setlen);
            lua_pop(lua, 2);
            return;
        }
        lua_pop(lua, 1); /* Discard field name pushed before. */

        /* Handle the array reply. */
        ValkeyModule_ReplyWithArray(ctx, VALKEYMODULE_POSTPONED_LEN);
        int j = 1, mbulklen = 0;
        while (1) {
            /* we took care of the stack size on function start */
            lua_pushnumber(lua, j++);
            lua_rawget(lua, -2);
            t = lua_type(lua, -1);
            if (t == LUA_TNIL) {
                lua_pop(lua, 1);
                break;
            }
            luajitReplyToServerReply(ctx, resp_version, lua);
            mbulklen++;
        }
        ValkeyModule_ReplySetArrayLength(ctx, mbulklen);
        break;
    default: ValkeyModule_ReplyWithNull(ctx);
    }
    lua_pop(lua, 1);
}

/* ---------------------------------------------------------------------------
 * Lua server.* functions implementations.
 * ------------------------------------------------------------------------- */
void freeLuajitServerArgv(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc);

/* Return the number of digits of 'v' when converted to string in radix 10.
 * See ll2string() for more information. */
static uint32_t digits10(uint64_t v) {
    if (v < 10) return 1;
    if (v < 100) return 2;
    if (v < 1000) return 3;
    if (v < 1000000000000UL) {
        if (v < 100000000UL) {
            if (v < 1000000) {
                if (v < 10000) return 4;
                return 5 + (v >= 100000);
            }
            return 7 + (v >= 10000000UL);
        }
        if (v < 10000000000UL) {
            return 9 + (v >= 1000000000UL);
        }
        return 11 + (v >= 100000000000UL);
    }
    return 12 + digits10(v / 1000000000000UL);
}

/* Convert a unsigned long long into a string. Returns the number of
 * characters needed to represent the number.
 * If the buffer is not big enough to store the string, 0 is returned.
 *
 * Based on the following article (that apparently does not provide a
 * novel approach but only publicizes an already used technique):
 *
 * https://web.archive.org/web/20150427221229/https://www.facebook.com/notes/facebook-engineering/three-optimization-tips-for-c/10151361643253920 */
static int ull2string(char *dst, size_t dstlen, unsigned long long value) {
    static const char digits[201] = "0001020304050607080910111213141516171819"
                                    "2021222324252627282930313233343536373839"
                                    "4041424344454647484950515253545556575859"
                                    "6061626364656667686970717273747576777879"
                                    "8081828384858687888990919293949596979899";

    /* Check length. */
    uint32_t length = digits10(value);
    if (length >= dstlen) goto err;

    /* Null term. */
    uint32_t next = length - 1;
    dst[next + 1] = '\0';
    while (value >= 100) {
        int const i = (value % 100) * 2;
        value /= 100;
        dst[next] = digits[i + 1];
        dst[next - 1] = digits[i];
        next -= 2;
    }

    /* Handle last 1-2 digits. */
    if (value < 10) {
        dst[next] = '0' + (uint32_t)value;
    } else {
        int i = (uint32_t)value * 2;
        dst[next] = digits[i + 1];
        dst[next - 1] = digits[i];
    }
    return length;
err:
    /* force add Null termination */
    if (dstlen > 0) dst[0] = '\0';
    return 0;
}

/* Convert a long long into a string. Returns the number of
 * characters needed to represent the number.
 * If the buffer is not big enough to store the string, 0 is returned. */
static int ll2string(char *dst, size_t dstlen, long long svalue) {
    unsigned long long value;
    int negative = 0;

    /* The ull2string function with 64bit unsigned integers for simplicity, so
     * we convert the number here and remember if it is negative. */
    if (svalue < 0) {
        if (svalue != LLONG_MIN) {
            value = -svalue;
        } else {
            value = ((unsigned long long)LLONG_MAX) + 1;
        }
        if (dstlen < 2) goto err;
        negative = 1;
        dst[0] = '-';
        dst++;
        dstlen--;
    } else {
        value = svalue;
    }

    /* Converts the unsigned long long value to string*/
    int length = ull2string(dst, dstlen, value);
    if (length == 0) return 0;
    return length + negative;

err:
    /* force add Null termination */
    if (dstlen > 0) dst[0] = '\0';
    return 0;
}

/* Returns 1 if the double value can safely be represented in long long without
 * precision loss, in which case the corresponding long long is stored in the out variable. */
static int double2ll(double d, long long *out) {
#if (__DBL_MANT_DIG__ >= 52) && (__DBL_MANT_DIG__ <= 63) && (LLONG_MAX == 0x7fffffffffffffffLL)
    /* Check if the float is in a safe range to be casted into a
     * long long. We are assuming that long long is 64 bit here.
     * Also we are assuming that there are no implementations around where
     * double has precision < 52 bit.
     *
     * Under this assumptions we test if a double is inside a range
     * where casting to long long is safe. Then using two castings we
     * make sure the decimal part is zero. If all this is true we can use
     * integer without precision loss.
     *
     * Note that numbers above 2^52 and below 2^63 use all the fraction bits as real part,
     * and the exponent bits are positive, which means the "decimal" part must be 0.
     * i.e. all double values in that range are representable as a long without precision loss,
     * but not all long values in that range can be represented as a double.
     * we only care about the first part here. */
    if (d < (double)(-LLONG_MAX / 2) || d > (double)(LLONG_MAX / 2)) return 0;
    long long ll = d;
    if (ll == d) {
        *out = ll;
        return 1;
    }
#else
    VALKEYMODULE_NOT_USED(d);
    VALKEYMODULE_NOT_USED(out);
#endif
    return 0;
}

static ValkeyModuleString **luajitArgsToServerArgv(ValkeyModuleCtx *ctx, lua_State *lua, int *argc) {
    int j;
    /* Require at least one argument */
    *argc = lua_gettop(lua);
    if (*argc == 0) {
        luajitPushError(lua, "Please specify at least one argument for this call");
        return NULL;
    }

    ValkeyModuleString **lua_argv = ValkeyModule_Alloc(sizeof(ValkeyModuleString *) * *argc);

    for (j = 0; j < *argc; j++) {
        char *obj_s;
        size_t obj_len;
        char dbuf[64];

        if (lua_type(lua, j + 1) == LUA_TNUMBER) {
            /* We can't use lua_tolstring() for number -> string conversion
             * since Lua uses a format specifier that loses precision. */
            lua_Number num = lua_tonumber(lua, j + 1);
            /* Integer printing function is much faster, check if we can safely use it.
             * Since lua_Number is not explicitly an integer or a double, we need to make an effort
             * to convert it as an integer when that's possible, since the string could later be used
             * in a context that doesn't support scientific notation (e.g. 1e9 instead of 100000000). */
            long long lvalue;
            if (double2ll((double)num, &lvalue)) {
                obj_len = ll2string(dbuf, sizeof(dbuf), lvalue);
            } else {
                obj_len = fpconv_dtoa((double)num, dbuf);
                dbuf[obj_len] = '\0';
            }
            obj_s = dbuf;
        } else {
            obj_s = (char *)lua_tolstring(lua, j + 1, &obj_len);
            if (obj_s == NULL) break;
        }

        lua_argv[j] = ValkeyModule_CreateString(ctx, obj_s, obj_len);
    }

    /* Pop all arguments from the stack, we do not need them anymore
     * and this way we guaranty we will have room on the stack for the result. */
    lua_pop(lua, *argc);

    /* Check if one of the arguments passed by the Lua script
     * is not a string or an integer (lua_isstring() return true for
     * integers as well). */
    if (j != *argc) {
        freeLuajitServerArgv(ctx, lua_argv, j);
        luajitPushError(lua, "Command arguments must be strings or integers");
        return NULL;
    }

    return lua_argv;
}

void freeLuajitServerArgv(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    int j;
    for (j = 0; j < argc; j++) {
        ValkeyModuleString *o = argv[j];
        ValkeyModule_FreeString(ctx, o);
    }
    ValkeyModule_Free(argv);
}

static void luajitProcessReplyError(ValkeyModuleCallReply *reply, lua_State *lua) {
    const char *err = ValkeyModule_CallReplyStringPtr(reply, NULL);
    int push_error = 1;

    /* The following error messages rewrites are required to keep the backward compatibility
     * with the previous Lua engine that was implemented in Valkey core. */
    if (errno == ESPIPE) {
        if (strncmp(err, "ERR command ", strlen("ERR command ")) == 0) {
            luajitPushError(lua, "This Valkey command is not allowed from script");
            push_error = 0;
        }
    } else if (errno == EINVAL) {
        if (strncmp(err, "ERR wrong number of arguments for ", strlen("ERR wrong number of arguments for ")) == 0) {
            luajitPushError(lua, "Wrong number of args calling command from script");
            push_error = 0;
        }
    } else if (errno == ENOENT) {
        if (strncmp(err, "ERR unknown command '", strlen("ERR unknown command '")) == 0) {
            luajitPushError(lua, "Unknown command called from script");
            push_error = 0;
        }
    } else if (errno == EACCES) {
        if (strncmp(err, "NOPERM ", strlen("NOPERM ")) == 0) {
            const char *err_prefix = "ACL failure in script: ";
            size_t err_len = strlen(err_prefix) + strlen(err + strlen("NOPERM ")) + 1;
            char *err_msg = ValkeyModule_Alloc(err_len * sizeof(char));
            memset(err_msg, 0, err_len);
            strcpy(err_msg, err_prefix);
            strcat(err_msg, err + strlen("NOPERM "));
            luajitPushError(lua, err_msg);
            ValkeyModule_Free(err_msg);
            push_error = 0;
        }
    }

    if (push_error) {
        /* The reply parser strips the leading '-' from the RESP error, so `err`
         * is of the form "CODE msg" (e.g. "OOM command not allowed..."). Re-add
         * the '-' so luajitPushErrorBuff() treats the leading word as the error
         * code and doesn't prepend another "ERR" code. */
        char *err_with_dash = ljm_asprintf("-%s", err);
        luajitPushError(lua, err_with_dash);
        ValkeyModule_Free(err_with_dash);
    }
    /* push a field indicate to ignore updating the stats on this error
     * because it was already updated when executing the command. */
    lua_pushstring(lua, "ignore_error_stats_update");
    lua_pushboolean(lua, 1);
    lua_settable(lua, -3);
}

static int luajitServerGenericCommand(lua_State *lua, int raise_error) {
    luajitFuncCallCtx *rctx = luajitGetFromRegistry(lua, REGISTRY_RUN_CTX_NAME);
    ValkeyModule_Assert(rctx);
    ValkeyModuleCallReply *reply;

    int argc = 0;
    ValkeyModuleString **argv = luajitArgsToServerArgv(rctx->module_ctx, lua, &argc);
    if (argv == NULL) {
        return raise_error ? luajitError(lua) : 1;
    }

    static int inuse = 0;

    /* Recursive calls detection. */

    /* By using Lua debug hooks it is possible to trigger a recursive call
     * to luajitServerGenericCommand(), which normally should never happen.
     * To make this function reentrant is futile and makes it slower, but
     * we should at least detect such a misuse, and abort. */
    if (inuse) {
        char *recursion_warning = "luaRedisGenericCommand() recursive call detected. "
                                  "Are you doing funny stuff with Lua debug hooks?";
        ValkeyModule_Log(rctx->module_ctx, "warning", "%s", recursion_warning);
        luajitPushError(lua, recursion_warning);
        return 1;
    }
    inuse++;

    char fmt[13] = "v!EMSX";
    int fmt_idx = 6;

    ValkeyModuleString *username = ValkeyModule_GetCurrentUserName(rctx->module_ctx);
    if (username != NULL) {
        fmt[fmt_idx++] = 'C';
        ValkeyModule_FreeString(rctx->module_ctx, username);
    }

    if (!(rctx->replication_flags & PROPAGATE_AOF)) {
        fmt[fmt_idx++] = 'A';
    }
    if (!(rctx->replication_flags & PROPAGATE_REPL)) {
        fmt[fmt_idx++] = 'R';
    }
    if (!rctx->replication_flags) {
        fmt[fmt_idx++] = 'A';
        fmt[fmt_idx++] = 'R';
    }
    if (rctx->resp == 3) {
        fmt[fmt_idx++] = '3';
    }
    fmt[fmt_idx] = '\0';

    const char *cmdname = ValkeyModule_StringPtrLen(argv[0], NULL);

    errno = 0;
    reply = ValkeyModule_Call(rctx->module_ctx, cmdname, fmt, argv + 1, argc - 1);
    freeLuajitServerArgv(rctx->module_ctx, argv, argc);
    int reply_type = ValkeyModule_CallReplyType(reply);
    if (errno != 0) {
        ValkeyModule_Assert(reply_type == VALKEYMODULE_REPLY_ERROR);

        const char *err = ValkeyModule_CallReplyStringPtr(reply, NULL);
        ValkeyModule_Log(rctx->module_ctx, "debug", "command returned an error: %s errno=%d", err, errno);

        luajitProcessReplyError(reply, lua);
        goto cleanup;
    } else if (raise_error && reply_type != VALKEYMODULE_REPLY_ERROR) {
        raise_error = 0;
    }

    callReplyToLuaType(lua, reply, rctx->resp);

cleanup:
    /* Clean up. Command code may have changed argv/argc so we use the
     * argv/argc of the client instead of the local variables. */
    ValkeyModule_FreeCallReply(reply);

    inuse--;

    if (raise_error) {
        /* If we are here we should have an error in the stack, in the
         * form of a table with an "err" field. Extract the string to
         * return the plain error. */
        return luajitError(lua);
    }
    return 1;
}

/* Our implementation to lua pcall.
 * We need this implementation for backward
 * comparability with older Redis OSS versions.
 *
 * On Redis OSS 7, the error object is a table,
 * compare to older version where the error
 * object is a string. To keep backward
 * comparability we catch the table object
 * and just return the error message. */
static int luajitRedisPcall(lua_State *lua) {
    int argc = lua_gettop(lua);
    lua_pushboolean(lua, 1);
    lua_insert(lua, 1);
    if (lua_pcall(lua, argc - 1, LUA_MULTRET, 0)) {
        lua_remove(lua, 1);
        if (lua_istable(lua, -1)) {
            lua_getfield(lua, -1, "err");
            if (lua_isstring(lua, -1)) {
                lua_replace(lua, -2);
            }
        }
        lua_pushboolean(lua, 0);
        lua_insert(lua, 1);
    }
    return lua_gettop(lua);
}

/* server.call() */
static int luajitRedisCallCommand(lua_State *lua) {
    return luajitServerGenericCommand(lua, 1);
}

/* server.pcall() */
static int luajitRedisPCallCommand(lua_State *lua) {
    return luajitServerGenericCommand(lua, 0);
}

/* Perform the SHA1 of the input string. We use this both for hashing script
 * bodies in order to obtain the Lua function name, and in the implementation
 * of server.sha1().
 *
 * 'digest' should point to a 41 bytes buffer: 40 for SHA1 converted into an
 * hexadecimal number, plus 1 byte for null term. */
static void sha1hex(char *digest, char *script, size_t len) {
    SHA1_CTX ctx;
    unsigned char hash[20];
    char *cset = "0123456789abcdef";
    int j;

    SHA1Init(&ctx);
    SHA1Update(&ctx, (unsigned char *)script, len);
    SHA1Final(hash, &ctx);

    for (j = 0; j < 20; j++) {
        digest[j * 2] = cset[((hash[j] & 0xF0) >> 4)];
        digest[j * 2 + 1] = cset[(hash[j] & 0xF)];
    }
    digest[40] = '\0';
}

/* This adds server.sha1hex(string) to Lua scripts using the same hashing
 * function used for sha1ing lua scripts. */
static int luajitRedisSha1hexCommand(lua_State *lua) {
    int argc = lua_gettop(lua);
    char digest[41];
    size_t len;
    char *s;

    if (argc != 1) {
        luajitPushError(lua, "wrong number of arguments");
        return luajitError(lua);
    }

    s = (char *)lua_tolstring(lua, 1, &len);
    sha1hex(digest, s, len);
    lua_pushstring(lua, digest);
    return 1;
}

/* Returns a table with a single field 'field' set to the string value
 * passed as argument. This helper function is handy when returning
 * a RESP error or status reply from Lua:
 *
 * return server.error_reply("ERR Some Error")
 * return server.status_reply("ERR Some Error")
 */
static int luajitRedisReturnSingleFieldTable(lua_State *lua, char *field) {
    if (lua_gettop(lua) != 1 || lua_type(lua, -1) != LUA_TSTRING) {
        luajitPushError(lua, "wrong number or type of arguments");
        return 1;
    }

    lua_newtable(lua);
    lua_pushstring(lua, field);
    lua_pushvalue(lua, -3);
    lua_settable(lua, -3);
    return 1;
}

/* server.error_reply() */
static int luajitRedisErrorReplyCommand(lua_State *lua) {
    if (lua_gettop(lua) != 1 || lua_type(lua, -1) != LUA_TSTRING) {
        luajitPushError(lua, "wrong number or type of arguments");
        return 1;
    }

    /* add '-' if not exists */
    const char *err = lua_tostring(lua, -1);
    if (!err) {
        luajitPushError(lua, "ERR unable to convert error to string");
        return 1;
    }
    char *err_buff = NULL;
    if (err[0] != '-') {
        err_buff = ljm_asprintf("-%s", err);
    } else {
        err_buff = ljm_strcpy(err);
    }
    luajitPushErrorBuff(lua, err_buff);
    ValkeyModule_Free(err_buff);
    return 1;
}

/* server.status_reply() */
static int luajitRedisStatusReplyCommand(lua_State *lua) {
    return luajitRedisReturnSingleFieldTable(lua, "ok");
}

/* server.set_repl()
 *
 * Set the propagation of write commands executed in the context of the
 * script to on/off for AOF and replicas. */
static int luajitRedisSetReplCommand(lua_State *lua) {
    int flags, argc = lua_gettop(lua);

    luajitFuncCallCtx *rctx = luajitGetFromRegistry(lua, REGISTRY_RUN_CTX_NAME);
    ValkeyModule_Assert(rctx); /* Only supported inside script invocation */

    if (argc != 1) {
        luajitPushError(lua, "server.set_repl() requires one argument.");
        return luajitError(lua);
    }

    flags = lua_tonumber(lua, -1);
    if ((flags & ~(PROPAGATE_AOF | PROPAGATE_REPL)) != 0) {
        luajitPushError(lua, "Invalid replication flags. Use REPL_AOF, REPL_REPLICA, REPL_ALL or REPL_NONE.");
        return luajitError(lua);
    }

    rctx->replication_flags = flags;

    return 0;
}

/* server.acl_check_cmd()
 *
 * Checks ACL permissions for given command for the current user. */
static int luajitRedisAclCheckCmdPermissionsCommand(lua_State *lua) {
    luajitFuncCallCtx *rctx = luajitGetFromRegistry(lua, REGISTRY_RUN_CTX_NAME);
    ValkeyModule_Assert(rctx);

    int raise_error = 0;

    int argc = 0;
    ValkeyModuleString **argv = luajitArgsToServerArgv(rctx->module_ctx, lua, &argc);

    /* Require at least one argument */
    if (argv == NULL) return luajitError(lua);

    ValkeyModuleString *username = ValkeyModule_GetCurrentUserName(rctx->module_ctx);
    ValkeyModuleUser *user = ValkeyModule_GetModuleUserFromUserName(username);
    int dbid = ValkeyModule_GetSelectedDb(rctx->module_ctx);
    ValkeyModule_FreeString(rctx->module_ctx, username);

    if (ValkeyModule_ACLCheckPermissions(user, argv, argc, dbid, NULL) != VALKEYMODULE_OK) {
        if (errno == EINVAL) {
            luajitPushError(lua, "Invalid command passed to server.acl_check_cmd()");
            raise_error = 1;
        } else {
            ValkeyModule_Assert(errno == EACCES);
            lua_pushboolean(lua, 0);
        }
    } else {
        lua_pushboolean(lua, 1);
    }

    ValkeyModule_FreeModuleUser(user);
    freeLuajitServerArgv(rctx->module_ctx, argv, argc);
    if (raise_error)
        return luajitError(lua);
    else
        return 1;
}

/* server.log() */
static int luajitLogCommand(lua_State *lua) {
    luajitFuncCallCtx *rctx = luajitGetFromRegistry(lua, REGISTRY_RUN_CTX_NAME);
    ValkeyModule_Assert(rctx);

    int j, argc = lua_gettop(lua);
    int level;

    if (argc < 2) {
        luajitPushError(lua, "server.log() requires two arguments or more.");
        return luajitError(lua);
    } else if (!lua_isnumber(lua, -argc)) {
        luajitPushError(lua, "First argument must be a number (log level).");
        return luajitError(lua);
    }
    level = lua_tonumber(lua, -argc);
    if (level < LL_DEBUG || level > LL_WARNING) {
        luajitPushError(lua, "Invalid log level.");
        return luajitError(lua);
    }

    char *log = NULL;
    for (j = 1; j < argc; j++) {
        size_t len;
        char *s;

        s = (char *)lua_tolstring(lua, (-argc) + j, &len);
        if (s) {
            if (j != 1) {
                char *next_log = ljm_asprintf("%s %s", log, s);
                ValkeyModule_Free(log);
                log = next_log;
            } else {
                log = ljm_asprintf("%s", s);
            }
        }
    }

    const char *level_str = NULL;
    switch (level) {
    case LL_DEBUG: level_str = "debug"; break;
    case LL_VERBOSE: level_str = "verbose"; break;
    case LL_NOTICE: level_str = "notice"; break;
    case LL_WARNING: level_str = "warning"; break;
    default: ValkeyModule_Assert(0);
    }

    ValkeyModule_Log(rctx->module_ctx, level_str, "%s", log);
    ValkeyModule_Free(log);
    return 0;
}

/* server.setresp() */
static int luajitSetResp(lua_State *lua) {
    luajitFuncCallCtx *rctx = luajitGetFromRegistry(lua, REGISTRY_RUN_CTX_NAME);
    ValkeyModule_Assert(rctx);

    int argc = lua_gettop(lua);

    if (argc != 1) {
        luajitPushError(lua, "server.setresp() requires one argument.");
        return luajitError(lua);
    }

    int resp = lua_tonumber(lua, -argc);
    if (resp != 2 && resp != 3) {
        luajitPushError(lua, "RESP version must be 2 or 3.");
        return luajitError(lua);
    }

    rctx->resp = resp;

    return 0;
}

extern int luaopen_cjson(lua_State *lua);
extern int luaopen_cjson_safe(lua_State *lua);
extern int luaopen_struct(lua_State *lua);
extern int luaopen_cmsgpack(lua_State *lua);
extern int luaopen_cmsgpack_safe(lua_State *lua);

/* ---------------------------------------------------------------------------
 * Lua engine initialization and reset.
 * ------------------------------------------------------------------------- */

static void luajitLoadLib(lua_State *lua, const char *libname, lua_CFunction luafunc) {
    lua_pushcfunction(lua, luafunc);
    lua_pushstring(lua, libname);
    lua_call(lua, 1, 0);
}

static void luajitWrapLoadFunction(lua_State *lua) {
    const char *wrap_code =
        "do "
        "  local load_original = load "
        "  load = function(reader, chunkname, mode, env) "
        "    return load_original(reader, chunkname, 't', env) "
        "  end "
        "  loadstring = load "
        "end";

    if (luaL_dostring(lua, wrap_code) != LUA_OK) {
        const char *err = lua_tostring(lua, -1);
        ValkeyModule_Log(NULL, "warning", "Failed to wrap load() function: %s", err ? err : "unknown");
        lua_pop(lua, 1);
    }
}

static void luajitLoadLibraries(lua_State *lua) {
    luajitLoadLib(lua, "", luaopen_base);
    luajitLoadLib(lua, LUA_TABLIBNAME, luaopen_table);
    luajitLoadLib(lua, LUA_STRLIBNAME, luaopen_string);
    luajitLoadLib(lua, LUA_MATHLIBNAME, luaopen_math);
    luajitLoadLib(lua, LUA_DBLIBNAME, luaopen_debug);
    luajitLoadLib(lua, LUA_OSLIBNAME, luaopen_os);

    lua_getglobal(lua, "os");
    lua_getfield(lua, -1, "clock");
    lua_newtable(lua);
    lua_pushvalue(lua, -2);
    lua_setfield(lua, -2, "clock");
    lua_setglobal(lua, "os");
    lua_pop(lua, 2);

    luajitLoadLib(lua, "bit", luaopen_bit);

    lua_pushcfunction(lua, luaopen_cjson);
    lua_call(lua, 0, 1);
    lua_setglobal(lua, "cjson");

    lua_pushcfunction(lua, luaopen_cjson_safe);
    lua_call(lua, 0, 1);
    lua_setglobal(lua, "cjson.safe");

    lua_pushcfunction(lua, luaopen_struct);
    lua_call(lua, 0, 1);
    lua_setglobal(lua, "struct");

    lua_pushcfunction(lua, luaopen_cmsgpack);
    lua_call(lua, 0, 1);
    lua_setglobal(lua, "cmsgpack");

    lua_pushcfunction(lua, luaopen_cmsgpack_safe);
    lua_call(lua, 0, 1);
    lua_setglobal(lua, "cmsgpack_safe");

    luajitWrapLoadFunction(lua);
}

static void luajitRemoveUnsafeGlobals(lua_State *lua) {
    char *deny_list[] = {
        "dofile",
        "loadfile",
        "print",
        "setfenv",
        "getfenv",
        "newproxy",
        NULL};

    for (char **p = deny_list; *p != NULL; p++) {
        lua_pushnil(lua);
        lua_setglobal(lua, *p);
    }
}

void luajitRegisterVersion(luajitEngineCtx *ctx, lua_State *lua) {
    lua_pushstring(lua, "REDIS_VERSION_NUM");
    lua_pushnumber(lua, ctx->redis_version_num);
    lua_settable(lua, -3);

    lua_pushstring(lua, "REDIS_VERSION");
    lua_pushstring(lua, ctx->redis_version);
    lua_settable(lua, -3);

    lua_pushstring(lua, "VALKEY_VERSION_NUM");
    lua_pushnumber(lua, ctx->valkey_version_num);
    lua_settable(lua, -3);

    lua_pushstring(lua, "VALKEY_VERSION");
    lua_pushstring(lua, ctx->valkey_version);
    lua_settable(lua, -3);

    lua_pushstring(lua, "SERVER_NAME");
    lua_pushstring(lua, ctx->server_name);
    lua_settable(lua, -3);
}

void luajitRegisterLogFunction(lua_State *lua) {
    lua_pushstring(lua, "log");
    lua_pushcfunction(lua, luajitLogCommand);
    lua_settable(lua, -3);

    lua_pushstring(lua, "LOG_DEBUG");
    lua_pushnumber(lua, LL_DEBUG);
    lua_settable(lua, -3);

    lua_pushstring(lua, "LOG_VERBOSE");
    lua_pushnumber(lua, LL_VERBOSE);
    lua_settable(lua, -3);

    lua_pushstring(lua, "LOG_NOTICE");
    lua_pushnumber(lua, LL_NOTICE);
    lua_settable(lua, -3);

    lua_pushstring(lua, "LOG_WARNING");
    lua_pushnumber(lua, LL_WARNING);
    lua_settable(lua, -3);
}

void luajitRegisterServerAPI(luajitEngineCtx *ctx, lua_State *lua) {
    luajitLoadLibraries(lua);

    luajitRemoveUnsafeGlobals(lua);

    luajitSaveOnRegistry(lua, REGISTRY_RUN_CTX_NAME, NULL);

    lua_pushcfunction(lua, luajitRedisPcall);
    lua_setglobal(lua, "pcall");

    lua_newtable(lua);
    lua_pushstring(lua, "call");
    lua_pushcfunction(lua, luajitRedisCallCommand);
    lua_settable(lua, -3);
    lua_pushstring(lua, "pcall");
    lua_pushcfunction(lua, luajitRedisPCallCommand);
    lua_settable(lua, -3);

    luajitRegisterLogFunction(lua);

    luajitRegisterVersion(ctx, lua);

    lua_pushstring(lua, "setresp");
    lua_pushcfunction(lua, luajitSetResp);
    lua_settable(lua, -3);

    lua_pushstring(lua, "sha1hex");
    lua_pushcfunction(lua, luajitRedisSha1hexCommand);
    lua_settable(lua, -3);

    lua_pushstring(lua, "error_reply");
    lua_pushcfunction(lua, luajitRedisErrorReplyCommand);
    lua_settable(lua, -3);
    lua_pushstring(lua, "status_reply");
    lua_pushcfunction(lua, luajitRedisStatusReplyCommand);
    lua_settable(lua, -3);

    lua_pushstring(lua, "set_repl");
    lua_pushcfunction(lua, luajitRedisSetReplCommand);
    lua_settable(lua, -3);

    lua_pushstring(lua, "REPL_NONE");
    lua_pushnumber(lua, PROPAGATE_NONE);
    lua_settable(lua, -3);

    lua_pushstring(lua, "REPL_AOF");
    lua_pushnumber(lua, PROPAGATE_AOF);
    lua_settable(lua, -3);

    lua_pushstring(lua, "REPL_SLAVE");
    lua_pushnumber(lua, PROPAGATE_REPL);
    lua_settable(lua, -3);

    lua_pushstring(lua, "REPL_REPLICA");
    lua_pushnumber(lua, PROPAGATE_REPL);
    lua_settable(lua, -3);

    lua_pushstring(lua, "REPL_ALL");
    lua_pushnumber(lua, PROPAGATE_AOF | PROPAGATE_REPL);
    lua_settable(lua, -3);

    lua_pushstring(lua, "acl_check_cmd");
    lua_pushcfunction(lua, luajitRedisAclCheckCmdPermissionsCommand);
    lua_settable(lua, -3);

    lua_setglobal(lua, SERVER_API_NAME);
    lua_getglobal(lua, SERVER_API_NAME);
    lua_setglobal(lua, REDIS_API_NAME);

    lua_getglobal(lua, "math");
    lua_pushstring(lua, "random");
    lua_pushcfunction(lua, server_math_random);
    lua_settable(lua, -3);
    lua_pushstring(lua, "randomseed");
    lua_pushcfunction(lua, server_math_randomseed);
    lua_settable(lua, -3);
    lua_setglobal(lua, "math");

    lua_pushinteger(lua, 0);
    lua_setfield(lua, LUA_REGISTRYINDEX, "__gc_count");
    lua_pushinteger(lua, 0);
    lua_setfield(lua, LUA_REGISTRYINDEX, "__full_gc_count");
}

static void luajitCreateArray(lua_State *lua, ValkeyModuleString **elev, int elec) {
    int j;

    lua_createtable(lua, elec, 0);
    for (j = 0; j < elec; j++) {
        size_t len = 0;
        const char *str = ValkeyModule_StringPtrLen(elev[j], &len);
        lua_pushlstring(lua, str, len);
        lua_rawseti(lua, -2, j + 1);
    }
}

static int server_math_random(lua_State *L) {
    lua_Number r = (lua_Number)(serverLrand48() % SERVER_LRAND48_MAX) /
                   (lua_Number)SERVER_LRAND48_MAX;
    switch (lua_gettop(L)) {
    case 0: {
        lua_pushnumber(L, r);
        break;
    }
    case 1: {
        int u = luaL_checkint(L, 1);
        luaL_argcheck(L, 1 <= u, 1, "interval is empty");
        lua_pushnumber(L, floor(r * u) + 1);
        break;
    }
    case 2: {
        int l = luaL_checkint(L, 1);
        int u = luaL_checkint(L, 2);
        luaL_argcheck(L, l <= u, 2, "interval is empty");
        lua_pushnumber(L, floor(r * (u - l + 1)) + l);
        break;
    }
    default: return luaL_error(L, "wrong number of arguments");
    }
    return 1;
}

static int server_math_randomseed(lua_State *L) {
    serverSrand48(luaL_checkint(L, 1));
    return 0;
}

static void luajitMaskCountHook(lua_State *lua, lua_Debug *ar) {
    VALKEYMODULE_NOT_USED(ar);

    luajitFuncCallCtx *rctx = luajitGetFromRegistry(lua, REGISTRY_RUN_CTX_NAME);
    ValkeyModule_Assert(rctx);

    ValkeyModuleScriptingEngineExecutionState state = ValkeyModule_GetFunctionExecutionState(rctx->run_ctx);
    if (state == VMSE_STATE_KILLED) {
        char *err = NULL;
        if (rctx->type == VMSE_EVAL) {
            err = "Script killed by user with SCRIPT KILL.";
        } else {
            err = "Script killed by user with FUNCTION KILL.";
        }
        ValkeyModule_Log(NULL, "notice", "%s", err);

        lua_sethook(lua, luajitMaskCountHook, LUA_MASKLINE, 0);

        luajitPushError(lua, err);
        luajitError(lua);
    }
}

void luajitErrorInformationDiscard(errorInfo *err_info) {
    if (err_info->msg) ValkeyModule_Free(err_info->msg);
    if (err_info->source) ValkeyModule_Free(err_info->source);
    if (err_info->line) ValkeyModule_Free(err_info->line);
}

void luajitExtractErrorInformation(lua_State *lua, errorInfo *err_info) {
    if (lua_isstring(lua, -1)) {
        err_info->msg = ljm_asprintf("ERR %s", lua_tostring(lua, -1));
        err_info->line = NULL;
        err_info->source = NULL;
        err_info->ignore_err_stats_update = 0;
        return;
    }

    lua_getfield(lua, -1, "err");
    if (lua_isstring(lua, -1)) {
        err_info->msg = ljm_strcpy(lua_tostring(lua, -1));
    }
    lua_pop(lua, 1);

    lua_getfield(lua, -1, "source");
    if (lua_isstring(lua, -1)) {
        err_info->source = ljm_strcpy(lua_tostring(lua, -1));
    }
    lua_pop(lua, 1);

    lua_getfield(lua, -1, "line");
    if (lua_isstring(lua, -1)) {
        err_info->line = ljm_strcpy(lua_tostring(lua, -1));
    }
    lua_pop(lua, 1);

    lua_getfield(lua, -1, "ignore_error_stats_update");
    if (lua_isboolean(lua, -1)) {
        err_info->ignore_err_stats_update = lua_toboolean(lua, -1);
    }
    lua_pop(lua, 1);

    if (err_info->msg == NULL) {
        err_info->msg = ljm_strcpy("ERR unknown error");
    }
}

void luajitCallFunction(ValkeyModuleCtx *ctx,
                        ValkeyModuleScriptingEngineServerRuntimeCtx *run_ctx,
                        ValkeyModuleScriptingEngineSubsystemType type,
                        lua_State *lua,
                        ValkeyModuleString **keys,
                        size_t nkeys,
                        ValkeyModuleString **args,
                        size_t nargs,
                        int lua_enable_insecure_api) {
    int delhook = 0;

    luajitFuncCallCtx call_ctx = {
        .module_ctx = ctx,
        .run_ctx = run_ctx,
        .type = type,
        .replication_flags = PROPAGATE_AOF | PROPAGATE_REPL,
        .resp = 2,
        .lua_enable_insecure_api = lua_enable_insecure_api,
    };

    luajitSaveOnRegistry(lua, REGISTRY_RUN_CTX_NAME, &call_ctx);

    lua_sethook(lua, luajitMaskCountHook, LUA_MASKCOUNT, LUA_HOOK_CHECK_INTERVAL);
    delhook = 1;

    luajitCreateArray(lua, keys, nkeys);
    if (type == VMSE_EVAL) {
        lua_setglobal(lua, "KEYS");
    }
    luajitCreateArray(lua, args, nargs);
    if (type == VMSE_EVAL) {
        lua_setglobal(lua, "ARGV");
    }

    int err;
    if (type == VMSE_EVAL) {
        err = lua_pcall(lua, 0, 1, -2);
    } else {
        err = lua_pcall(lua, 2, 1, -4);
    }

    {
        int gc_count = 0;
        lua_getfield(lua, LUA_REGISTRYINDEX, "__gc_count");
        if (!lua_isnil(lua, -1)) {
            gc_count = lua_tointeger(lua, -1);
        }
        lua_pop(lua, 1);

        int full_gc_count = 0;
        lua_getfield(lua, LUA_REGISTRYINDEX, "__full_gc_count");
        if (!lua_isnil(lua, -1)) {
            full_gc_count = lua_tointeger(lua, -1);
        }
        lua_pop(lua, 1);

        gc_count++;
        full_gc_count++;

        if (gc_count >= LUA_GC_CYCLE_PERIOD) {
            lua_gc(lua, LUA_GCSTEP, LUA_GC_CYCLE_PERIOD);
            gc_count = 0;
        }

        if (full_gc_count >= LUA_FULL_GC_CYCLE) {
            lua_gc(lua, LUA_GCCOLLECT, 0);
            full_gc_count = 0;
        }

        lua_pushinteger(lua, gc_count);
        lua_setfield(lua, LUA_REGISTRYINDEX, "__gc_count");
        lua_pushinteger(lua, full_gc_count);
        lua_setfield(lua, LUA_REGISTRYINDEX, "__full_gc_count");
    }

    if (err) {
        if (!lua_istable(lua, -1)) {
            const char *msg = "execution failure";
            if (lua_isstring(lua, -1)) {
                msg = lua_tostring(lua, -1);
            }
            ValkeyModule_ReplyWithErrorFormat(ctx, "ERR Error running script, %.100s\n", msg);
        } else {
            errorInfo err_info = {0};
            luajitExtractErrorInformation(lua, &err_info);
            if (err_info.line && err_info.source) {
                ValkeyModule_ReplyWithCustomErrorFormat(
                    ctx,
                    !err_info.ignore_err_stats_update,
                    "%s script: on %s:%s.",
                    err_info.msg,
                    err_info.source,
                    err_info.line);
            } else {
                ValkeyModule_ReplyWithCustomErrorFormat(
                    ctx,
                    !err_info.ignore_err_stats_update,
                    "%s",
                    err_info.msg);
            }
            luajitErrorInformationDiscard(&err_info);
        }
        lua_pop(lua, 1);
    } else {
        luajitReplyToServerReply(ctx, call_ctx.resp, lua);
    }

    if (delhook) lua_sethook(lua, NULL, 0, 0);

    luajitSaveOnRegistry(lua, REGISTRY_RUN_CTX_NAME, NULL);
}

unsigned long luajitMemory(lua_State *lua) {
    return lua_gc(lua, LUA_GCCOUNT, 0) * 1024LL;
}

int luajitUserStateRegisterFunction(lua_State *lua) {
    int argc = lua_gettop(lua);
    const char *name = NULL;

    if (argc < 1 || argc > 2) {
        lua_pushstring(lua, "wrong number of arguments to server.register_function");
        return lua_error(lua);
    }

    lua_getfield(lua, LUA_REGISTRYINDEX, "__library_functions");

    if (lua_isnil(lua, -1)) {
        lua_pop(lua, 1);
        lua_newtable(lua);
        lua_setfield(lua, LUA_REGISTRYINDEX, "__library_functions");
        lua_getfield(lua, LUA_REGISTRYINDEX, "__library_functions");
    }

    if (argc == 1) {
        if (!lua_istable(lua, 1)) {
            lua_pushstring(lua, "calling server.register_function with a single argument is only "
                                "applicable to Lua table (representing named arguments)");
            return lua_error(lua);
        }

        lua_getfield(lua, 1, "function_name");
        if (!lua_isstring(lua, -1)) {
            lua_pop(lua, 2);
            lua_pushstring(lua, "function_name argument given to server.register_function must be a string");
            return lua_error(lua);
        }
        name = lua_tostring(lua, -1);
        lua_pop(lua, 1);

        lua_getfield(lua, 1, "callback");
        if (!lua_isfunction(lua, -1)) {
            lua_pop(lua, 2);
            lua_pushstring(lua, "callback argument given to server.register_function must be a function");
            return lua_error(lua);
        }
        lua_remove(lua, 1);

    } else if (argc == 2) {
        if (!lua_isstring(lua, 1)) {
            lua_pop(lua, 3);
            lua_pushstring(lua, "function name must be a string");
            return lua_error(lua);
        }
        name = lua_tostring(lua, 1);

        if (!lua_isfunction(lua, 2)) {
            lua_pop(lua, 3);
            lua_pushstring(lua, "function argument must be a function");
            return lua_error(lua);
        }

        lua_remove(lua, 1);
        lua_pushvalue(lua, 1);
        lua_remove(lua, 1);
    }

    lua_setfield(lua, 1, name);
    lua_pop(lua, 1);
    return 0;
}

int luajitCompileLibraryInUserState(lua_State *lua,
                                    const char *code,
                                    size_t code_len,
                                    const char *library_name) {
    if (luaL_loadbuffer(lua, code, code_len, library_name) != 0) {
        return -1;
    }

    if (lua_pcall(lua, 0, 0, 0) != 0) {
        return -1;
    }

    return 0;
}

void luajitRemoveFunctionFromUserState(lua_State *lua, const char *function_name) {
    lua_getfield(lua, LUA_REGISTRYINDEX, "__library_functions");
    if (lua_istable(lua, -1)) {
        lua_pushnil(lua);
        lua_setfield(lua, -2, function_name);
    }
    lua_pop(lua, 1);
}
