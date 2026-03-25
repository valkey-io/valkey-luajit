/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * LuaJIT scripting engine module for Valkey.
 *
 * Per-user architecture: each authenticated user gets their own pair of
 * lua_State instances (one for EVAL, one for FUNCTION), created lazily on
 * first use. Scripts are compiled and stored as source text during
 * compile_code (which has no user identity) and loaded into the appropriate
 * per-user state during call_function.
 *
 * This eliminates the need for readonly-table protection (which is
 * incompatible with LuaJIT) because users are isolated by separate states.
 */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "valkeymodule.h"
#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>
#include <string.h>
#include <errno.h>

#include "engine_structs.h"
#include "function_luajit.h"
#include "script_luajit.h"
#include "ffi_lua_embedded.h"

#include <dlfcn.h>

extern int luaopen_ffi(lua_State *L);
extern int luaopen_jit(lua_State *L);

#define LUA_ENGINE_NAME "LUA"
#define REGISTRY_FUNC_CACHE_NAME "__func_cache"

static int luajitFFIGetCurrentContext(lua_State *lua) {
    lua_getfield(lua, LUA_REGISTRYINDEX, "__ffi_ctx");
    return 1;
}
#define REGISTRY_ERROR_HANDLER_NAME "__ERROR_HANDLER__"

/* Adds server.replicate_commands()
 *
 * DEPRECATED: Now do nothing and always return true.
 * Turn on single commands replication if the script never called
 * a write command so far, and returns true. Otherwise if the script
 * already started to write, returns false and stick to whole scripts
 * replication, which is our default. */
static int luajitServerReplicateCommandsCommand(lua_State *lua) {
    lua_pushboolean(lua, 1);
    return 1;
}

static int luajitServerBreakpointCommand(lua_State *lua) {
    lua_pushboolean(lua, 0);
    return 1;
}

static int luajitServerDebugCommand(lua_State *lua) {
    (void)lua;
    return 0;
}

/* Add a helper function we use for pcall error reporting.
 * Note that when the error is in the C function we want to report the
 * information about the caller, that's what makes sense from the point
 * of view of the user debugging a script. */
static void luajitStateInstallErrorHandler(lua_State *lua) {
    lua_pushstring(lua, REGISTRY_ERROR_HANDLER_NAME);
    char *errh_func = "local dbg = debug\n"
                      "debug = nil\n"
                      "local error_handler = function (err)\n"
                      "  local i = dbg.getinfo(2,'nSl')\n"
                      "  if i and i.what == 'C' then\n"
                      "    i = dbg.getinfo(3,'nSl')\n"
                      "  end\n"
                      "  if type(err) ~= 'table' then\n"
                      "    err = {err='ERR ' .. tostring(err)}"
                      "  end"
                      "  if i then\n"
                      "    err['source'] = i.source\n"
                      "    err['line'] = i.currentline\n"
                      "  end"
                      "  return err\n"
                      "end\n"
                      "return error_handler";
    luaL_loadbuffer(lua, errh_func, strlen(errh_func), "@err_handler_def");
    lua_pcall(lua, 0, 1, 0);
    lua_settable(lua, LUA_REGISTRYINDEX);
}

static void initializeEvalExtras(lua_State *lua) {
    lua_getglobal(lua, "server");

    lua_pushstring(lua, "breakpoint");
    lua_pushcfunction(lua, luajitServerBreakpointCommand);
    lua_settable(lua, -3);

    lua_pushstring(lua, "debug");
    lua_pushcfunction(lua, luajitServerDebugCommand);
    lua_settable(lua, -3);

    lua_pushstring(lua, "replicate_commands");
    lua_pushcfunction(lua, luajitServerReplicateCommandsCommand);
    lua_settable(lua, -3);

    lua_setglobal(lua, "server");

    lua_pushstring(lua, REGISTRY_ERROR_HANDLER_NAME);
    lua_gettable(lua, LUA_REGISTRYINDEX);
    lua_setglobal(lua, "__server__err__handler");
    lua_setglobal(lua, "__redis__err__handler");
}

static uint32_t parse_semver(const char *version) {
    unsigned int major = 0, minor = 0, patch = 0;
    sscanf(version, "%u.%u.%u", &major, &minor, &patch);
    return ((major & 0xFF) << 16) | ((minor & 0xFF) << 8) | (patch & 0xFF);
}

static void get_version_info(ValkeyModuleCtx *ctx,
                             char **redis_version,
                             uint32_t *redis_version_num,
                             char **server_name,
                             char **valkey_version,
                             uint32_t *valkey_version_num) {
    ValkeyModuleServerInfoData *info = ValkeyModule_GetServerInfo(ctx, "server");
    ValkeyModule_Assert(info != NULL);

    const char *rv = ValkeyModule_ServerInfoGetFieldC(info, "redis_version");
    *redis_version = ljm_strcpy(rv);
    *redis_version_num = parse_semver(*redis_version);

    const char *sn = ValkeyModule_ServerInfoGetFieldC(info, "server_name");
    *server_name = ljm_strcpy(sn);

    const char *vv = ValkeyModule_ServerInfoGetFieldC(info, "valkey_version");
    *valkey_version = ljm_strcpy(vv);
    *valkey_version_num = parse_semver(*valkey_version);

    ValkeyModule_FreeServerInfo(ctx, info);
}

static void luajitLoadFFI(lua_State *lua) {
    lua_pushcfunction(lua, luaopen_ffi);
    lua_pushstring(lua, "ffi");
    lua_call(lua, 1, 0);
    lua_getfield(lua, LUA_REGISTRYINDEX, "_LOADED");
    if (lua_istable(lua, -1)) {
        lua_getfield(lua, -1, "ffi");
        lua_setglobal(lua, "ffi");
    }
    lua_pop(lua, 1);

    lua_pushcfunction(lua, luaopen_jit);
    lua_pushstring(lua, "jit");
    lua_call(lua, 1, 0);

    lua_pushcfunction(lua, luajitFFIGetCurrentContext);
    lua_setglobal(lua, "__vkm_get_ctx");

    {
        Dl_info dl_info;
        if (dladdr((void *)luajitLoadFFI, &dl_info) && dl_info.dli_fname) {
            lua_pushstring(lua, dl_info.dli_fname);
            lua_setglobal(lua, "__vkm_module_path");
        }
    }

    if (luaL_dostring(lua, ffi_cdef_lua) != 0) {
        const char *err = lua_tostring(lua, -1);
        fprintf(stderr, "WARNING: FFI cdef init failed: %s\n", err ? err : "unknown");
        lua_pop(lua, 1);
        return;
    }
    lua_setglobal(lua, "__ffi_cdef_str");

    if (luaL_dostring(lua, ffi_api_lua) != 0) {
        const char *err = lua_tostring(lua, -1);
        fprintf(stderr, "WARNING: FFI API init failed: %s\n", err ? err : "unknown");
        lua_pop(lua, 1);
        lua_pushnil(lua);
        lua_setglobal(lua, "__ffi_cdef_str");
        return;
    }
    lua_setglobal(lua, "VKM");

    lua_pushnil(lua);
    lua_setglobal(lua, "__ffi_cdef_str");
    lua_pushnil(lua);
    lua_setglobal(lua, "__vkm_module_path");
}

static lua_State *createUserLuaState(luajitEngineCtx *engine_ctx,
                                     ValkeyModuleScriptingEngineSubsystemType type) {
    lua_State *lua = luaL_newstate();

    luajitRegisterServerAPI(engine_ctx, lua);
    luajitStateInstallErrorHandler(lua);

    if (type == VMSE_EVAL) {
        initializeEvalExtras(lua);
    }

    if (engine_ctx->enable_ffi_api) {
        luajitLoadFFI(lua);
    }

    lua_pushstring(lua, REGISTRY_FUNC_CACHE_NAME);
    lua_newtable(lua);
    lua_settable(lua, LUA_REGISTRYINDEX);

    if (type == VMSE_FUNCTION) {
        lua_newtable(lua);
        lua_setfield(lua, LUA_REGISTRYINDEX, "__library_functions");

        lua_newtable(lua);
        lua_setfield(lua, LUA_REGISTRYINDEX, "__library_compiled");

        lua_getglobal(lua, SERVER_API_NAME);
        lua_pushstring(lua, "register_function");
        lua_pushcfunction(lua, luajitUserStateRegisterFunction);
        lua_settable(lua, -3);
        lua_pop(lua, 1);
    }

    if (engine_ctx->enable_ffi_api) {
        lua_pushlightuserdata(lua, NULL);
        lua_setfield(lua, LUA_REGISTRYINDEX, "__ffi_ctx");
    }

    return lua;
}

static luajitPerUserState *createPerUserState(luajitEngineCtx *engine_ctx) {
    luajitPerUserState *us = ValkeyModule_Calloc(1, sizeof(*us));
    us->eval_lua = createUserLuaState(engine_ctx, VMSE_EVAL);
    us->function_lua = createUserLuaState(engine_ctx, VMSE_FUNCTION);
    return us;
}

static void destroyPerUserState(luajitPerUserState *us) {
    if (us->eval_lua) {
        lua_close(us->eval_lua);
    }
    if (us->function_lua) {
        lua_close(us->function_lua);
    }
    ValkeyModule_Free(us);
}

static luajitPerUserState *getOrCreateUserState(luajitEngineCtx *engine_ctx,
                                                const char *username) {
    luajitPerUserState *us = ValkeyModule_DictGetC(engine_ctx->user_states,
                                                   (void *)username,
                                                   strlen(username),
                                                   NULL);
    if (us) return us;

    us = createPerUserState(engine_ctx);
    ValkeyModule_DictSetC(engine_ctx->user_states,
                          (void *)username,
                          strlen(username),
                          us);
    return us;
}

static lua_State *createCompileScratchState(luajitEngineCtx *engine_ctx) {
    lua_State *lua = luaL_newstate();
    luaL_openlibs(lua);
    (void)engine_ctx;
    return lua;
}

static luajitEngineCtx *createEngineContext(ValkeyModuleCtx *ctx) {
    luajitEngineCtx *engine_ctx = ValkeyModule_Calloc(1, sizeof(*engine_ctx));

    get_version_info(ctx,
                     &engine_ctx->redis_version,
                     &engine_ctx->redis_version_num,
                     &engine_ctx->server_name,
                     &engine_ctx->valkey_version,
                     &engine_ctx->valkey_version_num);

    engine_ctx->lua_enable_insecure_api = 0;
    engine_ctx->enable_ffi_api = 0;
    engine_ctx->next_func_id = 0;
    engine_ctx->next_lib_id = 0;

    engine_ctx->compile_lua = createCompileScratchState(engine_ctx);
    engine_ctx->user_states = ValkeyModule_CreateDict(NULL);
    engine_ctx->libraries = ValkeyModule_CreateDict(NULL);

    return engine_ctx;
}

static void destroyEngineContext(luajitEngineCtx *engine_ctx) {
    ValkeyModuleDictIter *iter = ValkeyModule_DictIteratorStartC(
        engine_ctx->user_states, "^", NULL, 0);

    char *key;
    size_t keylen;
    luajitPerUserState *us;
    while ((key = ValkeyModule_DictNextC(iter, &keylen, (void **)&us)) != NULL) {
        destroyPerUserState(us);
    }
    ValkeyModule_DictIteratorStop(iter);
    ValkeyModule_FreeDict(NULL, engine_ctx->user_states);

    ValkeyModuleDictIter *lib_iter = ValkeyModule_DictIteratorStartC(engine_ctx->libraries, "^", NULL, 0);
    luajitLibrary *lib;
    while (ValkeyModule_DictNextC(lib_iter, &keylen, (void **)&lib) != NULL) {
        ValkeyModule_Free(lib->code);
        ValkeyModule_Free(lib->name);
        ValkeyModule_Free(lib);
    }
    ValkeyModule_DictIteratorStop(lib_iter);
    ValkeyModule_FreeDict(NULL, engine_ctx->libraries);

    if (engine_ctx->compile_lua) lua_close(engine_ctx->compile_lua);

    ValkeyModule_Free(engine_ctx->redis_version);
    ValkeyModule_Free(engine_ctx->server_name);
    ValkeyModule_Free(engine_ctx->valkey_version);
    ValkeyModule_Free(engine_ctx);
}

static ValkeyModuleScriptingEngineMemoryInfo luajitEngineGetMemoryInfo(
    ValkeyModuleCtx *module_ctx,
    ValkeyModuleScriptingEngineCtx *engine_ctx_opaque,
    ValkeyModuleScriptingEngineSubsystemType type) {
    VALKEYMODULE_NOT_USED(module_ctx);
    luajitEngineCtx *ctx = (luajitEngineCtx *)engine_ctx_opaque;
    ValkeyModuleScriptingEngineMemoryInfo mem_info = {0};

    ValkeyModuleDictIter *iter = ValkeyModule_DictIteratorStartC(
        ctx->user_states, "^", NULL, 0);

    char *key;
    size_t keylen;
    luajitPerUserState *us;
    while ((key = ValkeyModule_DictNextC(iter, &keylen, (void **)&us)) != NULL) {
        if ((type == VMSE_EVAL || type == VMSE_ALL) && us->eval_lua) {
            mem_info.used_memory += luajitMemory(us->eval_lua);
        }
        if ((type == VMSE_FUNCTION || type == VMSE_ALL) && us->function_lua) {
            mem_info.used_memory += luajitMemory(us->function_lua);
        }
    }
    ValkeyModule_DictIteratorStop(iter);

    if (ctx->compile_lua) {
        mem_info.used_memory += luajitMemory(ctx->compile_lua);
    }

    if (type == VMSE_FUNCTION || type == VMSE_ALL) {
        ValkeyModuleDictIter *lib_iter = ValkeyModule_DictIteratorStartC(ctx->libraries, "^", NULL, 0);
        luajitLibrary *lib;
        while (ValkeyModule_DictNextC(lib_iter, &keylen, (void **)&lib) != NULL) {
            mem_info.used_memory += lib->code_len;
            mem_info.used_memory += ValkeyModule_MallocSize(lib->code);
            mem_info.used_memory += ValkeyModule_MallocSize(lib->name);
            mem_info.used_memory += ValkeyModule_MallocSize(lib);
        }
        ValkeyModule_DictIteratorStop(lib_iter);
    }

    mem_info.engine_memory_overhead = ValkeyModule_MallocSize(engine_ctx_opaque);

    return mem_info;
}

static ValkeyModuleScriptingEngineCompiledFunction **luajitEngineCompileCode(
    ValkeyModuleCtx *module_ctx,
    ValkeyModuleScriptingEngineCtx *engine_ctx_opaque,
    ValkeyModuleScriptingEngineSubsystemType type,
    const char *code,
    size_t code_len,
    size_t timeout,
    size_t *out_num_compiled_functions,
    ValkeyModuleString **err) {
    luajitEngineCtx *ctx = (luajitEngineCtx *)engine_ctx_opaque;
    ValkeyModuleScriptingEngineCompiledFunction **functions = NULL;

    if (type == VMSE_EVAL) {
        lua_State *lua = ctx->compile_lua;

        if (luaL_loadbuffer(lua, code, code_len, "@user_script")) {
            *err = ValkeyModule_CreateStringPrintf(
                module_ctx,
                "Error compiling script (new function): %s",
                lua_tostring(lua, -1));
            lua_pop(lua, 1);
            return NULL;
        }

        ValkeyModule_Assert(lua_isfunction(lua, -1));
        lua_pop(lua, 1);

        luajitFunction *func_data = ValkeyModule_Calloc(1, sizeof(*func_data));
        func_data->is_from_eval = 1;
        func_data->source.text = ValkeyModule_Alloc(code_len);
        memcpy(func_data->source.text, code, code_len);
        func_data->source.text_len = code_len;

        ValkeyModuleScriptingEngineCompiledFunction *func =
            ValkeyModule_Calloc(1, sizeof(*func));
        func->name = NULL;
        func->function = func_data;
        func->desc = NULL;
        func->f_flags = 0;

        *out_num_compiled_functions = 1;
        functions = ValkeyModule_Calloc(1, sizeof(*functions));
        *functions = func;
    } else {
        lua_State *scratch = luaL_newstate();
        luajitInitFunctionScratchState(ctx, scratch);

        functions = luajitFunctionLibraryCreate(scratch,
                                                code,
                                                code_len,
                                                timeout,
                                                out_num_compiled_functions,
                                                err);

        if (functions) {
            luajitLibrary *lib = ValkeyModule_Calloc(1, sizeof(*lib));
            lib->code = ValkeyModule_Alloc(code_len);
            memcpy(lib->code, code, code_len);
            lib->code_len = code_len;
            lib->lib_id = ++ctx->next_lib_id;
            lib->name = ljm_asprintf("lib_%lu", lib->lib_id);
            lib->ref_count = *out_num_compiled_functions;

            for (size_t i = 0; i < *out_num_compiled_functions; i++) {
                luajitFunction *func = (luajitFunction *)functions[i]->function;
                func->function_ref.lib_id = lib->lib_id;
            }

            ValkeyModule_DictSetC(ctx->libraries,
                                  (char *)&lib->lib_id,
                                  sizeof(lib->lib_id),
                                  lib);
        }

        lua_close(scratch);
    }

    if (functions) {
        for (size_t i = 0; i < *out_num_compiled_functions; i++) {
            luajitFunction *fd =
                (luajitFunction *)functions[i]->function;
            fd->func_id = ++ctx->next_func_id;
        }
    }

    return functions;
}

static void luajitEngineFunctionCall(
    ValkeyModuleCtx *module_ctx,
    ValkeyModuleScriptingEngineCtx *engine_ctx_opaque,
    ValkeyModuleScriptingEngineServerRuntimeCtx *server_ctx,
    ValkeyModuleScriptingEngineCompiledFunction *compiled_function,
    ValkeyModuleScriptingEngineSubsystemType type,
    ValkeyModuleString **keys,
    size_t nkeys,
    ValkeyModuleString **args,
    size_t nargs) {
    luajitEngineCtx *ctx = (luajitEngineCtx *)engine_ctx_opaque;

    ValkeyModuleString *username_str = ValkeyModule_GetCurrentUserName(module_ctx);
    size_t uname_len;
    const char *username = ValkeyModule_StringPtrLen(username_str, &uname_len);

    luajitPerUserState *us = getOrCreateUserState(ctx, username);
    ValkeyModule_FreeString(NULL, username_str);

    lua_State *lua = (type == VMSE_EVAL) ? us->eval_lua : us->function_lua;
    luajitFunction *func = (luajitFunction *)compiled_function->function;

    lua_pushstring(lua, REGISTRY_FUNC_CACHE_NAME);
    lua_gettable(lua, LUA_REGISTRYINDEX);

    lua_pushlightuserdata(lua, (void *)(uintptr_t)func->func_id);
    lua_rawget(lua, -2);

    if (lua_isfunction(lua, -1)) {
        lua_remove(lua, -2);
    } else {
        lua_pop(lua, 1);

        const char *chunk_name = (type == VMSE_EVAL) ? "@user_script" : "@user_function";
        if (func->is_from_eval) {
            if (luaL_loadbuffer(lua, func->source.text, func->source.text_len, chunk_name) != 0) {
                const char *errmsg = lua_tostring(lua, -1);
                ValkeyModule_ReplyWithErrorFormat(module_ctx,
                                                  "ERR Error loading script: %s",
                                                  errmsg ? errmsg : "unknown error");
                lua_pop(lua, 2);
                return;
            }
        } else {
            int nokey;
            luajitLibrary *lib = ValkeyModule_DictGetC(
                ctx->libraries,
                (char *)&func->function_ref.lib_id,
                sizeof(func->function_ref.lib_id),
                &nokey);

            if (lib == NULL || nokey) {
                ValkeyModule_ReplyWithErrorFormat(module_ctx,
                                                  "ERR Library for function %s not found",
                                                  func->function_ref.name);
                lua_pop(lua, 2);
                return;
            }

            lua_getfield(lua, LUA_REGISTRYINDEX, "__library_compiled");
            lua_pushnumber(lua, (lua_Number)func->function_ref.lib_id);
            lua_gettable(lua, -2);
            int is_compiled = !lua_isnil(lua, -1);
            lua_pop(lua, 1);
            lua_pop(lua, 1);

            if (!is_compiled) {
                const char *lib_name;
                char lib_name_buf[64];
                if (strlen(lib->name) < sizeof(lib_name_buf)) {
                    strcpy(lib_name_buf, lib->name);
                    lib_name = lib_name_buf;
                } else {
                    lib_name = lib->name;
                }

                if (luajitCompileLibraryInUserState(lua, lib->code, lib->code_len, lib_name) != 0) {
                    const char *errmsg = lua_tostring(lua, -1);
                    ValkeyModule_ReplyWithErrorFormat(module_ctx,
                                                      "ERR Error compiling library %s: %s",
                                                      lib_name,
                                                      errmsg ? errmsg : "unknown error");
                    lua_pop(lua, 2);
                    return;
                }

                lua_getfield(lua, LUA_REGISTRYINDEX, "__library_compiled");
                lua_pushnumber(lua, (lua_Number)func->function_ref.lib_id);
                lua_pushboolean(lua, 1);
                lua_settable(lua, -3);
                lua_pop(lua, 1);
            }

            lua_getfield(lua, LUA_REGISTRYINDEX, "__library_functions");
            lua_getfield(lua, -1, func->function_ref.name);

            if (lua_isnil(lua, -1) || !lua_isfunction(lua, -1)) {
                lua_pop(lua, 3);
                ValkeyModule_ReplyWithErrorFormat(module_ctx,
                                                  "ERR Function %s not found in library",
                                                  func->function_ref.name);
                return;
            }

            lua_remove(lua, -2);
        }

        lua_pushlightuserdata(lua, (void *)(uintptr_t)func->func_id);
        lua_pushvalue(lua, -2);
        lua_rawset(lua, -4);

        lua_remove(lua, -2);
    }

    lua_pushstring(lua, REGISTRY_ERROR_HANDLER_NAME);
    lua_gettable(lua, LUA_REGISTRYINDEX);

    lua_insert(lua, -2);

    if (ctx->enable_ffi_api) {
        lua_pushlightuserdata(lua, module_ctx);
        lua_setfield(lua, LUA_REGISTRYINDEX, "__ffi_ctx");
    }

    luajitCallFunction(module_ctx,
                       server_ctx,
                       type,
                       lua,
                       keys,
                       nkeys,
                       args,
                       nargs,
                       ctx->lua_enable_insecure_api);

    if (ctx->enable_ffi_api) {
        lua_pushlightuserdata(lua, NULL);
        lua_setfield(lua, LUA_REGISTRYINDEX, "__ffi_ctx");
    }

    lua_pop(lua, 1);
}

static void luajitEngineFreeFunction(
    ValkeyModuleCtx *module_ctx,
    ValkeyModuleScriptingEngineCtx *engine_ctx_opaque,
    ValkeyModuleScriptingEngineSubsystemType type,
    ValkeyModuleScriptingEngineCompiledFunction *compiled_function) {
    VALKEYMODULE_NOT_USED(module_ctx);
    VALKEYMODULE_NOT_USED(type);
    luajitEngineCtx *ctx = (luajitEngineCtx *)engine_ctx_opaque;
    luajitFunction *func = (luajitFunction *)compiled_function->function;

    if (func->is_from_eval) {
        ValkeyModule_Free(func->source.text);
    } else {
        const char *func_name = func->function_ref.name;

        ValkeyModuleDictIter *iter = ValkeyModule_DictIteratorStartC(
            ctx->user_states, "^", NULL, 0);

        char *key;
        size_t keylen;
        luajitPerUserState *us;
        while ((key = ValkeyModule_DictNextC(iter, &keylen, (void **)&us)) != NULL) {
            if (us->function_lua) {
                luajitRemoveFunctionFromUserState(us->function_lua, func_name);
            }
        }
        ValkeyModule_DictIteratorStop(iter);

        int nokey;
        luajitLibrary *lib = ValkeyModule_DictGetC(
            ctx->libraries,
            (char *)&func->function_ref.lib_id,
            sizeof(func->function_ref.lib_id),
            &nokey);

        if (lib && !nokey) {
            lib->ref_count--;
            if (lib->ref_count == 0) {
                ValkeyModule_Free(lib->code);
                ValkeyModule_Free(lib->name);
                ValkeyModule_Free(lib);
                ValkeyModule_DictDelC(ctx->libraries,
                                      (char *)&func->function_ref.lib_id,
                                      sizeof(func->function_ref.lib_id),
                                      NULL);
            }
        }

        ValkeyModule_Free(func->function_ref.name);
    }
    ValkeyModule_Free(func);

    if (compiled_function->name) ValkeyModule_FreeString(NULL, compiled_function->name);
    if (compiled_function->desc) ValkeyModule_FreeString(NULL, compiled_function->desc);
    ValkeyModule_Free(compiled_function);
}

static size_t luajitEngineFunctionMemoryOverhead(
    ValkeyModuleCtx *module_ctx,
    ValkeyModuleScriptingEngineCompiledFunction *compiled_function) {
    VALKEYMODULE_NOT_USED(module_ctx);
    luajitFunction *func = (luajitFunction *)compiled_function->function;
    size_t data_len = func->is_from_eval ? func->source.text_len : (strlen(func->function_ref.name) + 1);
    return data_len +
           ValkeyModule_MallocSize(func) +
           (compiled_function->name ? ValkeyModule_MallocSize(compiled_function->name) : 0) +
           (compiled_function->desc ? ValkeyModule_MallocSize(compiled_function->desc) : 0) +
           ValkeyModule_MallocSize(compiled_function);
}

typedef struct resetCtx {
    luajitPerUserState **states;
    size_t count;
} resetCtx;

static void resetLuaContextAsync(void *context) {
    resetCtx *rctx = context;
    for (size_t i = 0; i < rctx->count; i++) {
        destroyPerUserState(rctx->states[i]);
    }
    ValkeyModule_Free(rctx->states);
    ValkeyModule_Free(rctx);
}

static int isLuaInsecureAPIEnabled(ValkeyModuleCtx *module_ctx) {
    int result = 0;
    ValkeyModuleCallReply *reply =
        ValkeyModule_Call(module_ctx, "CONFIG", "ccE", "GET", "lua-enable-insecure-api");
    if (ValkeyModule_CallReplyType(reply) == VALKEYMODULE_REPLY_ERROR) {
        ValkeyModule_FreeCallReply(reply);
        return 0;
    }
    ValkeyModule_Assert(ValkeyModule_CallReplyType(reply) == VALKEYMODULE_REPLY_ARRAY &&
                        ValkeyModule_CallReplyLength(reply) == 2);
    ValkeyModuleCallReply *val = ValkeyModule_CallReplyArrayElement(reply, 1);
    ValkeyModule_Assert(ValkeyModule_CallReplyType(val) == VALKEYMODULE_REPLY_STRING);
    const char *val_str = ValkeyModule_CallReplyStringPtr(val, NULL);
    result = strncmp(val_str, "yes", 3) == 0;
    ValkeyModule_FreeCallReply(reply);
    return result;
}

static ValkeyModuleScriptingEngineCallableLazyEnvReset *luajitEngineResetEnv(
    ValkeyModuleCtx *module_ctx,
    ValkeyModuleScriptingEngineCtx *engine_ctx_opaque,
    ValkeyModuleScriptingEngineSubsystemType type,
    int async) {
    VALKEYMODULE_NOT_USED(module_ctx);
    luajitEngineCtx *ctx = (luajitEngineCtx *)engine_ctx_opaque;
    ValkeyModuleScriptingEngineCallableLazyEnvReset *callback = NULL;

    if (type == VMSE_FUNCTION || type == VMSE_ALL) {
        ValkeyModuleDictIter *lib_iter = ValkeyModule_DictIteratorStartC(
            ctx->libraries, "^", NULL, 0);
        char *key;
        size_t keylen;
        luajitLibrary *lib;
        while ((key = ValkeyModule_DictNextC(lib_iter, &keylen, (void **)&lib)) != NULL) {
            ValkeyModule_Free(lib->code);
            ValkeyModule_Free(lib->name);
            ValkeyModule_Free(lib);
        }
        ValkeyModule_DictIteratorStop(lib_iter);
        ValkeyModule_FreeDict(NULL, ctx->libraries);
        ctx->libraries = ValkeyModule_CreateDict(NULL);

        ctx->next_lib_id = 0;
    }

    size_t n = 0;
    luajitPerUserState **old_states = NULL;

    ValkeyModuleDictIter *iter = ValkeyModule_DictIteratorStartC(
        ctx->user_states, "^", NULL, 0);

    char *key;
    size_t keylen;
    luajitPerUserState *us;

    while ((key = ValkeyModule_DictNextC(iter, &keylen, (void **)&us)) != NULL) {
        n++;
    }
    ValkeyModule_DictIteratorStop(iter);

    if (n > 0) {
        old_states = ValkeyModule_Calloc(n, sizeof(*old_states));
        size_t idx = 0;
        iter = ValkeyModule_DictIteratorStartC(ctx->user_states, "^", NULL, 0);
        while ((key = ValkeyModule_DictNextC(iter, &keylen, (void **)&us)) != NULL) {
            if (type == VMSE_ALL) {
                old_states[idx++] = us;
            } else {
                luajitPerUserState *partial = ValkeyModule_Calloc(1, sizeof(*partial));
                if (type == VMSE_EVAL) {
                    partial->eval_lua = us->eval_lua;
                    us->eval_lua = createUserLuaState(ctx, VMSE_EVAL);
                } else {
                    partial->function_lua = us->function_lua;
                    us->function_lua = createUserLuaState(ctx, VMSE_FUNCTION);
                }
                old_states[idx++] = partial;
            }
        }
        ValkeyModule_DictIteratorStop(iter);

        if (type == VMSE_ALL) {
            ValkeyModule_FreeDict(NULL, ctx->user_states);
            ctx->user_states = ValkeyModule_CreateDict(NULL);
        }
    }

    if (async && n > 0) {
        resetCtx *rctx = ValkeyModule_Alloc(sizeof(*rctx));
        rctx->states = old_states;
        rctx->count = n;
        callback = ValkeyModule_Calloc(1, sizeof(*callback));
        *callback = (ValkeyModuleScriptingEngineCallableLazyEnvReset){
            .context = rctx,
            .engineLazyEnvResetCallback = resetLuaContextAsync,
        };
    } else if (n > 0) {
        for (size_t i = 0; i < n; i++) {
            destroyPerUserState(old_states[i]);
        }
        ValkeyModule_Free(old_states);
    }

    if (ctx->compile_lua) {
        lua_close(ctx->compile_lua);
        ctx->compile_lua = createCompileScratchState(ctx);
    }

    ctx->lua_enable_insecure_api = isLuaInsecureAPIEnabled(module_ctx);

    return callback;
}

static ValkeyModuleScriptingEngineDebuggerEnableRet luajitEngineDebuggerEnable(
    ValkeyModuleCtx *module_ctx,
    ValkeyModuleScriptingEngineCtx *engine_ctx_opaque,
    ValkeyModuleScriptingEngineSubsystemType type,
    const ValkeyModuleScriptingEngineDebuggerCommand **commands,
    size_t *commands_len) {
    VALKEYMODULE_NOT_USED(module_ctx);
    VALKEYMODULE_NOT_USED(engine_ctx_opaque);
    VALKEYMODULE_NOT_USED(type);
    VALKEYMODULE_NOT_USED(commands);
    VALKEYMODULE_NOT_USED(commands_len);
    return VMSE_DEBUG_NOT_SUPPORTED;
}

static void luajitEngineDebuggerDisable(
    ValkeyModuleCtx *module_ctx,
    ValkeyModuleScriptingEngineCtx *engine_ctx_opaque,
    ValkeyModuleScriptingEngineSubsystemType type) {
    VALKEYMODULE_NOT_USED(module_ctx);
    VALKEYMODULE_NOT_USED(engine_ctx_opaque);
    VALKEYMODULE_NOT_USED(type);
}

static void luajitEngineDebuggerStart(
    ValkeyModuleCtx *module_ctx,
    ValkeyModuleScriptingEngineCtx *engine_ctx_opaque,
    ValkeyModuleScriptingEngineSubsystemType type,
    ValkeyModuleString *source) {
    VALKEYMODULE_NOT_USED(module_ctx);
    VALKEYMODULE_NOT_USED(engine_ctx_opaque);
    VALKEYMODULE_NOT_USED(type);
    VALKEYMODULE_NOT_USED(source);
}

static void luajitEngineDebuggerEnd(
    ValkeyModuleCtx *module_ctx,
    ValkeyModuleScriptingEngineCtx *engine_ctx_opaque,
    ValkeyModuleScriptingEngineSubsystemType type) {
    VALKEYMODULE_NOT_USED(module_ctx);
    VALKEYMODULE_NOT_USED(engine_ctx_opaque);
    VALKEYMODULE_NOT_USED(type);
}

static int ffi_enabled = 0;

static int ffiGetConfig(const char *name, void *privdata) {
    VALKEYMODULE_NOT_USED(name);
    VALKEYMODULE_NOT_USED(privdata);
    return ffi_enabled;
}

static int ffiSetConfig(const char *name, int val, void *privdata, ValkeyModuleString **err) {
    VALKEYMODULE_NOT_USED(name);
    VALKEYMODULE_NOT_USED(privdata);
    VALKEYMODULE_NOT_USED(err);
    ffi_enabled = val;
    return VALKEYMODULE_OK;
}

static luajitEngineCtx *engine_ctx = NULL;

int ValkeyModule_OnLoad(ValkeyModuleCtx *ctx,
                        ValkeyModuleString **argv,
                        int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);

    if (ValkeyModule_Init(ctx, "luajit", 1, VALKEYMODULE_APIVER_1) == VALKEYMODULE_ERR) {
        return VALKEYMODULE_ERR;
    }

    ValkeyModule_SetModuleOptions(ctx, VALKEYMODULE_OPTIONS_HANDLE_REPL_ASYNC_LOAD |
                                           VALKEYMODULE_OPTIONS_HANDLE_ATOMIC_SLOT_MIGRATION);

    if (ValkeyModule_RegisterBoolConfig(ctx,
                                        "enable-ffi-api",
                                        0,
                                        VALKEYMODULE_CONFIG_IMMUTABLE,
                                        ffiGetConfig,
                                        ffiSetConfig,
                                        NULL,
                                        NULL) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to register enable-ffi-api config");
        return VALKEYMODULE_ERR;
    }

    engine_ctx = createEngineContext(ctx);

    if (ValkeyModule_LoadConfigs(ctx) == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to load LuaJIT module configs");
        destroyEngineContext(engine_ctx);
        engine_ctx = NULL;
        return VALKEYMODULE_ERR;
    }

    engine_ctx->enable_ffi_api = ffi_enabled;
    if (ffi_enabled) {
        ValkeyModule_Log(ctx, "warning",
                         "LuaJIT FFI API is enabled. Scripts have unrestricted access "
                         "to the process. This is NOT suitable for untrusted scripts.");
    }

    ValkeyModuleScriptingEngineMethods methods = {
        .version = VALKEYMODULE_SCRIPTING_ENGINE_ABI_VERSION,
        .compile_code = luajitEngineCompileCode,
        .free_function = luajitEngineFreeFunction,
        .call_function = luajitEngineFunctionCall,
        .get_function_memory_overhead = luajitEngineFunctionMemoryOverhead,
        .reset_env = luajitEngineResetEnv,
        .get_memory_info = luajitEngineGetMemoryInfo,
        .debugger_enable = luajitEngineDebuggerEnable,
        .debugger_disable = luajitEngineDebuggerDisable,
        .debugger_start = luajitEngineDebuggerStart,
        .debugger_end = luajitEngineDebuggerEnd,
    };

    int result = ValkeyModule_RegisterScriptingEngine(ctx,
                                                      LUA_ENGINE_NAME,
                                                      engine_ctx,
                                                      &methods);

    if (result == VALKEYMODULE_ERR) {
        ValkeyModule_Log(ctx, "warning", "Failed to register LUA scripting engine");
        destroyEngineContext(engine_ctx);
        engine_ctx = NULL;
        return VALKEYMODULE_ERR;
    }

    engine_ctx->lua_enable_insecure_api = isLuaInsecureAPIEnabled(ctx);

    ValkeyModule_Log(ctx, "notice",
                     "LuaJIT scripting engine registered as '%s' (per-user isolation)",
                     LUA_ENGINE_NAME);

    return VALKEYMODULE_OK;
}

int ValkeyModule_OnUnload(ValkeyModuleCtx *ctx) {
    if (ValkeyModule_UnregisterScriptingEngine(ctx, LUA_ENGINE_NAME) != VALKEYMODULE_OK) {
        ValkeyModule_Log(ctx, "error", "Failed to unregister LuaJIT engine");
        return VALKEYMODULE_ERR;
    }

    destroyEngineContext(engine_ctx);
    engine_ctx = NULL;

    ValkeyModule_Log(ctx, "notice", "LuaJIT scripting engine unloaded");

    return VALKEYMODULE_OK;
}
