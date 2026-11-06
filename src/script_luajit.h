/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * Shared scripting functionality for the LuaJIT engine module.
 * Provides:
 *   - Lua state initialization with server API registration
 *   - server.call()/server.pcall() implementation
 *   - Reply conversion (Lua <-> RESP)
 *   - Execution hooks (timeout/kill detection)
 *   - Library compilation helpers
 */

#ifndef __SCRIPT_LUAJIT_H_
#define __SCRIPT_LUAJIT_H_

#include "valkeymodule.h"
#include "engine_structs.h"

#define C_OK 0
#define C_ERR -1

typedef struct lua_State lua_State;

#define REGISTRY_RUN_CTX_NAME "__RUN_CTX__"
#define REGISTRY_MODULE_CTX_NAME "__MODULE_CTX__"
#define REDIS_API_NAME "redis"
#define SERVER_API_NAME "server"

#define LUA_HOOK_CHECK_INTERVAL 100000
#define LUA_GC_CYCLE_PERIOD 50
#define LUA_FULL_GC_CYCLE 500

typedef struct errorInfo {
    char *msg;
    char *source;
    char *line;
    int ignore_err_stats_update;
} errorInfo;

void luajitRegisterServerAPI(luajitEngineCtx *ctx, lua_State *lua);
void luajitRegisterLogFunction(lua_State *lua);
void luajitRegisterVersion(luajitEngineCtx *ctx, lua_State *lua);

void luajitPushError(lua_State *lua, const char *error);
int luajitError(lua_State *lua);

void luajitSaveOnRegistry(lua_State *lua, const char *name, void *ptr);
void *luajitGetFromRegistry(lua_State *lua, const char *name);

void luajitCallFunction(ValkeyModuleCtx *ctx,
                        ValkeyModuleScriptingEngineServerRuntimeCtx *r_ctx,
                        ValkeyModuleScriptingEngineSubsystemType type,
                        lua_State *lua,
                        ValkeyModuleString **keys,
                        size_t nkeys,
                        ValkeyModuleString **args,
                        size_t nargs,
                        luajitEngineCtx *engine_ctx);

void luajitExtractErrorInformation(lua_State *lua, errorInfo *err_info);
void luajitErrorInformationDiscard(errorInfo *err_info);

unsigned long luajitMemory(lua_State *lua);

struct ValkeyModuleDict *luajitCoreCommandsCreate(ValkeyModuleCtx *ctx);
void luajitArgvCacheFree(luajitEngineCtx *ectx);

void luajitWatchdogStart(void);
void luajitWatchdogStop(void);
void luajitWatchdogSetBusyThreshold(long long ms);

int luajitCompileLibraryInUserState(lua_State *lua,
                                    const char *code,
                                    size_t code_len,
                                    const char *library_name);

void luajitRemoveFunctionFromUserState(lua_State *lua, const char *function_name);

typedef struct luajitNamedArgKeys {
    const char *function_name;
    const char *description;
    const char *callback;
    const char *flags;
} luajitNamedArgKeys;

const char *luajitReadNamedArgKeys(lua_State *lua, luajitNamedArgKeys *keys);
void luajitPushNamedArg(lua_State *lua, const char *key);

int luajitUserStateRegisterFunction(lua_State *lua);

char *ljm_strcpy(const char *str);
char *ljm_strncpy(const char *str, size_t len);
char *ljm_asprintf(char const *fmt, ...);
#endif
