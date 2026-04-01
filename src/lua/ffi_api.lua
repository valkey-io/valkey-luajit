--[[
  ffi_api.lua – OOP Lua wrappers around the ValkeyModule FFI bindings.

  Provides:
    * VKM.ctx() – obtain the current ValkeyModuleCtx*
    * Helper functions – thin wrappers for common operations
    * ffi.metatype-based OOP – ValkeyModuleString, ValkeyModuleKey
    * VKM.C – raw access to all declared ValkeyModule API function pointers
    * Constants – status codes, key modes, key types, reply types

  Copyright (c) Valkey Contributors
  SPDX-License-Identifier: BSD-3-Clause
]]

local ffi = ffi
local cdef_str = __ffi_cdef_str
ffi.cdef(cdef_str)

local C
if __vkm_module_path then
    C = ffi.load(__vkm_module_path)
else
    C = ffi.C
end

local _len = ffi.new("size_t[1]")

local VKM = {}

VKM.OK  = 0
VKM.ERR = 1

VKM.READ              = 0x0001
VKM.WRITE             = 0x0002
VKM.OPEN_KEY_NOTOUCH  = 0x00010000
VKM.OPEN_KEY_NONOTIFY = 0x00020000
VKM.OPEN_KEY_NOSTATS  = 0x00040000
VKM.OPEN_KEY_NOEXPIRE = 0x00080000
VKM.OPEN_KEY_NOEFFECTS = 0x00100000

VKM.KEYTYPE_EMPTY   = 0
VKM.KEYTYPE_STRING  = 1
VKM.KEYTYPE_LIST    = 2
VKM.KEYTYPE_HASH    = 3
VKM.KEYTYPE_SET     = 4
VKM.KEYTYPE_ZSET    = 5
VKM.KEYTYPE_MODULE  = 6
VKM.KEYTYPE_STREAM  = 7

VKM.REPLY_UNKNOWN         = -1
VKM.REPLY_STRING          = 0
VKM.REPLY_ERROR           = 1
VKM.REPLY_INTEGER         = 2
VKM.REPLY_ARRAY           = 3
VKM.REPLY_NULL            = 4
VKM.REPLY_MAP             = 5
VKM.REPLY_SET             = 6
VKM.REPLY_BOOL            = 7
VKM.REPLY_DOUBLE          = 8
VKM.REPLY_BIG_NUMBER      = 9
VKM.REPLY_VERBATIM_STRING = 10
VKM.REPLY_ATTRIBUTE       = 11
VKM.REPLY_PROMISE         = 12
VKM.REPLY_SIMPLE_STRING   = 13
VKM.REPLY_ARRAY_NULL      = 14

VKM.POSTPONED_LEN = -1

VKM.LIST_HEAD = 0
VKM.LIST_TAIL = 1

VKM.HASH_NONE      = 0
VKM.HASH_NX        = 0x0001
VKM.HASH_XX        = 0x0002
VKM.HASH_CFIELDS   = 0x0004
VKM.HASH_EXISTS    = 0x0008
VKM.HASH_COUNT_ALL = 0x0010

VKM.ZADD_XX      = 0x0001
VKM.ZADD_NX      = 0x0002
VKM.ZADD_ADDED   = 0x0004
VKM.ZADD_UPDATED = 0x0008
VKM.ZADD_NOP     = 0x0010
VKM.ZADD_GT      = 0x0020
VKM.ZADD_LT      = 0x0040

VKM.STREAM_ADD_AUTOID         = 0x0001
VKM.STREAM_ITERATOR_EXCLUSIVE = 0x0001
VKM.STREAM_ITERATOR_REVERSE   = 0x0002
VKM.STREAM_TRIM_APPROX        = 0x0001

VKM.NO_EXPIRE = -1

VKM.POSITIVE_INFINITE = 1.0 / 0.0
VKM.NEGATIVE_INFINITE = -1.0 / 0.0

VKM.LOG_DEBUG   = "debug"
VKM.LOG_VERBOSE = "verbose"
VKM.LOG_NOTICE  = "notice"
VKM.LOG_WARNING = "warning"

VKM.YIELD_FLAG_NONE     = 0x00000001
VKM.YIELD_FLAG_CLIENTS  = 0x00000002

VKM.OPTIONS_HANDLE_IO_ERRORS = 0x0001
VKM.OPTION_NO_IMPLICIT_SIGNAL_MODIFIED = 0x0002
VKM.OPTIONS_HANDLE_REPL_ASYNC_LOAD = 0x0004
VKM.OPTIONS_ALLOW_NESTED_KEYSPACE_NOTIFICATIONS = 0x0008
VKM.OPTIONS_SKIP_COMMAND_VALIDATION = 0x0010
VKM.OPTIONS_HANDLE_ATOMIC_SLOT_MIGRATION = 0x0020

VKM.EVENT_REPLICATION_ROLE_CHANGED = 0
VKM.EVENT_PERSISTENCE = 1
VKM.EVENT_FLUSHDB = 2
VKM.EVENT_LOADING = 3
VKM.EVENT_CLIENT_CHANGE = 4
VKM.EVENT_SHUTDOWN = 5
VKM.EVENT_REPLICA_CHANGE = 6
VKM.EVENT_PRIMARY_LINK_CHANGE = 7
VKM.EVENT_CRON_LOOP = 8
VKM.EVENT_MODULE_CHANGE = 9
VKM.EVENT_LOADING_PROGRESS = 10
VKM.EVENT_SWAPDB = 11
VKM.EVENT_REPL_ASYNC_LOAD = 14
VKM.EVENT_FORK_CHILD = 13
VKM.EVENT_EVENTLOOP = 15
VKM.EVENT_CONFIG = 16
VKM.EVENT_KEY = 17
VKM.EVENT_AUTHENTICATION_ATTEMPT = 18
VKM.EVENT_ATOMIC_SLOT_MIGRATION = 19

VKM.AUX_BEFORE_RDB = 0x0001
VKM.AUX_AFTER_RDB = 0x0002

VKM.BLOCK_UNBLOCK_DEFAULT = 0
VKM.BLOCK_UNBLOCK_DELETED = 0x0001

VKM.CMDFILTER_NOSELF = 0x0001

VKM.CTX_FLAGS_LUA                    = 0x00000001
VKM.CTX_FLAGS_MULTI                  = 0x00000002
VKM.CTX_FLAGS_PRIMARY                = 0x00000004
VKM.CTX_FLAGS_REPLICA                = 0x00000008
VKM.CTX_FLAGS_READONLY               = 0x00000010
VKM.CTX_FLAGS_CLUSTER                = 0x00000020
VKM.CTX_FLAGS_AOF                    = 0x00000040
VKM.CTX_FLAGS_RDB                    = 0x00000080
VKM.CTX_FLAGS_MAXMEMORY              = 0x00000100
VKM.CTX_FLAGS_EVICT                  = 0x00000200
VKM.CTX_FLAGS_OOM                    = 0x00000400
VKM.CTX_FLAGS_OOM_WARNING            = 0x00000800
VKM.CTX_FLAGS_REPLICATED             = 0x00001000
VKM.CTX_FLAGS_LOADING                = 0x00002000
VKM.CTX_FLAGS_REPLICA_IS_STALE       = 0x00004000
VKM.CTX_FLAGS_REPLICA_IS_CONNECTING  = 0x00008000
VKM.CTX_FLAGS_REPLICA_IS_TRANSFERRING = 0x00010000
VKM.CTX_FLAGS_REPLICA_IS_ONLINE      = 0x00020000
VKM.CTX_FLAGS_ACTIVE_CHILD           = 0x00040000
VKM.CTX_FLAGS_MULTI_DIRTY            = 0x00080000
VKM.CTX_FLAGS_IS_CHILD               = 0x00100000
VKM.CTX_FLAGS_DENY_BLOCKING          = 0x00200000
VKM.CTX_FLAGS_RESP3                  = 0x00400000
VKM.CTX_FLAGS_ASYNC_LOADING          = 0x00800000
VKM.CTX_FLAGS_SERVER_STARTUP         = 0x01000000
VKM.CTX_FLAGS_SLOT_IMPORT_CLIENT     = 0x02000000
VKM.CTX_FLAGS_SLOT_EXPORT_CLIENT     = 0x04000000

VKM.NODE_ID_LEN = 40
VKM.NODE_MYSELF = 0x0001
VKM.NODE_PRIMARY = 0x0002
VKM.NODE_REPLICA = 0x0004
VKM.NODE_PFAIL = 0x0008
VKM.NODE_FAIL = 0x0010
VKM.NODE_NOFAILOVER = 0x0020

VKM.CLUSTER_FLAG_NONE = 0
VKM.CLUSTER_FLAG_NO_FAILOVER = 0x0002
VKM.CLUSTER_FLAG_NO_REDIRECTION = 0x0004

VKM.CLIENTINFO_FLAG_SSL                = 0x00000001
VKM.CLIENTINFO_FLAG_PUBSUB             = 0x00000002
VKM.CLIENTINFO_FLAG_BLOCKED            = 0x00000004
VKM.CLIENTINFO_FLAG_TRACKING           = 0x00000008
VKM.CLIENTINFO_FLAG_UNIXSOCKET         = 0x00000010
VKM.CLIENTINFO_FLAG_MULTI              = 0x00000020
VKM.CLIENTINFO_FLAG_READONLY           = 0x00000040
VKM.CLIENTINFO_FLAG_PRIMARY            = 0x00000080
VKM.CLIENTINFO_FLAG_REPLICA            = 0x00000100
VKM.CLIENTINFO_FLAG_MONITOR            = 0x00000200
VKM.CLIENTINFO_FLAG_MODULE             = 0x00000400
VKM.CLIENTINFO_FLAG_AUTHENTICATED      = 0x00000800
VKM.CLIENTINFO_FLAG_EVER_AUTHENTICATED = 0x00001000
VKM.CLIENTINFO_FLAG_FAKE               = 0x00002000

VKM.ACL_LOG_AUTH = 0
VKM.ACL_LOG_CMD = 1
VKM.ACL_LOG_KEY = 2
VKM.ACL_LOG_CHANNEL = 3
VKM.ACL_LOG_DB = 4

VKM.AUTH_HANDLED = 0
VKM.AUTH_NOT_HANDLED = 1

function VKM.ctx()
    local ud = __vkm_get_ctx()
    if ud == nil then return nil end
    return ffi.cast("ValkeyModuleCtx *", ud)
end

function VKM.reply_with_string(str)
    local ctx = VKM.ctx()
    if type(str) == "string" then
        return C.ValkeyModule_ReplyWithStringBuffer(ctx, str, #str)
    else
        return C.ValkeyModule_ReplyWithString(ctx, str)
    end
end

function VKM.reply_with_longlong(n)
    local ctx = VKM.ctx()
    return C.ValkeyModule_ReplyWithLongLong(ctx, n)
end

function VKM.reply_with_error(msg)
    local ctx = VKM.ctx()
    return C.ValkeyModule_ReplyWithError(ctx, msg)
end

function VKM.reply_with_cstring(str)
    local ctx = VKM.ctx()
    return C.ValkeyModule_ReplyWithCString(ctx, str)
end

function VKM.reply_with_empty_string()
    local ctx = VKM.ctx()
    return C.ValkeyModule_ReplyWithEmptyString(ctx)
end

function VKM.reply_with_empty_array()
    local ctx = VKM.ctx()
    return C.ValkeyModule_ReplyWithEmptyArray(ctx)
end

function VKM.reply_with_null_array()
    local ctx = VKM.ctx()
    return C.ValkeyModule_ReplyWithNullArray(ctx)
end

function VKM.reply_with_verbatim_string(str)
    local ctx = VKM.ctx()
    return C.ValkeyModule_ReplyWithVerbatimString(ctx, str, #str)
end

function VKM.reply_with_longdouble(ld)
    local ctx = VKM.ctx()
    return C.ValkeyModule_ReplyWithLongDouble(ctx, ld)
end

function VKM.reply_with_attribute(len)
    local ctx = VKM.ctx()
    return C.ValkeyModule_ReplyWithAttribute(ctx, len)
end

function VKM.reply_set_attribute_length(len)
    local ctx = VKM.ctx()
    return C.ValkeyModule_ReplySetAttributeLength(ctx, len)
end

function VKM.reply_set_push_length(len)
    local ctx = VKM.ctx()
    return C.ValkeyModule_ReplySetPushLength(ctx, len)
end

function VKM.reply_with_custom_error(update_error_stats, fmt, ...)
    local ctx = VKM.ctx()
    return C.ValkeyModule_ReplyWithCustomErrorFormat(ctx, update_error_stats and 1 or 0, fmt, ...)
end

function VKM.log(level, msg)
    local ctx = VKM.ctx()
    C.ValkeyModule_Log(ctx, level, "%s", msg)
end

function VKM.db_size()
    local ctx = VKM.ctx()
    return tonumber(C.ValkeyModule_DbSize(ctx))
end

function VKM.random_key()
    local ctx = VKM.ctx()
    if ctx == nil then return VKM.ERR end
    local rms = C.ValkeyModule_RandomKey(ctx)
    if rms == nil then
        return nil
    end

    local ptr = C.ValkeyModule_StringPtrLen(rms, _len)
    if ptr == nil then
        return nil
    end

    local result = ffi.string(ptr, _len[0])
    C.ValkeyModule_FreeString(ctx, rms)
    return result
end

function VKM.reset_dataset(restart_aof, async)
    C.ValkeyModule_ResetDataset(restart_aof and 1 or 0, async and 1 or 0)
end

function VKM.publish_message(channel, message)
    local ctx = VKM.ctx()
    local channel_str, message_str
    local free_channel, free_message = false, false

    if type(channel) == "string" then
        channel_str = C.ValkeyModule_CreateString(ctx, channel, #channel)
        free_channel = true
    else
        channel_str = channel
    end

    if type(message) == "string" then
        message_str = C.ValkeyModule_CreateString(ctx, message, #message)
        free_message = true
    else
        message_str = message
    end

    local result = C.ValkeyModule_PublishMessage(ctx, channel_str, message_str)

    if free_channel then C.ValkeyModule_FreeString(ctx, channel_str) end
    if free_message then C.ValkeyModule_FreeString(ctx, message_str) end

    return tonumber(result)
end

function VKM.publish_message_shard(channel, message)
    local ctx = VKM.ctx()
    local channel_str, message_str
    local free_channel, free_message = false, false

    if type(channel) == "string" then
        channel_str = C.ValkeyModule_CreateString(ctx, channel, #channel)
        free_channel = true
    else
        channel_str = channel
    end

    if type(message) == "string" then
        message_str = C.ValkeyModule_CreateString(ctx, message, #message)
        free_message = true
    else
        message_str = message
    end

    local result = C.ValkeyModule_PublishMessageShard(ctx, channel_str, message_str)

    if free_channel then C.ValkeyModule_FreeString(ctx, channel_str) end
    if free_message then C.ValkeyModule_FreeString(ctx, message_str) end

    return tonumber(result)
end

function VKM.get_my_cluster_id()
    local ptr = C.ValkeyModule_GetMyClusterID()
    if ptr == nil then return nil end
    return ffi.string(ptr)
end

function VKM.get_cluster_size()
    return tonumber(C.ValkeyModule_GetClusterSize())
end

function VKM.send_cluster_message(target_id, msg_type, msg)
    local ctx = VKM.ctx()
    return C.ValkeyModule_SendClusterMessage(ctx, target_id, msg_type, msg, #msg)
end

function VKM.get_cluster_node_info(id)
    local ctx = VKM.ctx()
    local ip = ffi.new("char[46]")
    local primary_id = ffi.new("char[?]", VKM.NODE_ID_LEN + 1)
    local port = ffi.new("int[1]")
    local flags = ffi.new("int[1]")
    local rc = C.ValkeyModule_GetClusterNodeInfo(ctx, id, ip, primary_id, port, flags)
    if rc ~= VKM.OK then
        return nil
    end
    return {
        id = id,
        ip = ffi.string(ip),
        primary_id = ffi.string(primary_id) or "",
        port = port[0],
        flags = tonumber(flags[0])
    }
end

function VKM.get_cluster_node_info_for_client(client_id, node_id)
    local ctx = VKM.ctx()
    local ip = ffi.new("char[46]")
    local primary_id = ffi.new("char[?]", VKM.NODE_ID_LEN + 1)
    local port = ffi.new("int[1]")
    local flags = ffi.new("int[1]")
    local rc = C.ValkeyModule_GetClusterNodeInfoForClient(ctx, client_id, node_id, ip, primary_id, port, flags)
    if rc ~= VKM.OK then
        return nil
    end
    return {
        node_id = node_id,
        ip = ffi.string(ip),
        primary_id = ffi.string(primary_id) or "",
        port = port[0],
        flags = tonumber(flags[0])
    }
end

function VKM.get_cluster_nodes_list()
    local ctx = VKM.ctx()
    local numnodes = ffi.new("size_t[1]")
    local ids = C.ValkeyModule_GetClusterNodesList(ctx, numnodes)
    if ids == nil then
        return nil
    end
    local result = {}
    for i = 0, numnodes[0] - 1 do
        result[i + 1] = ffi.string(ids[i])
    end
    C.ValkeyModule_FreeClusterNodesList(ids)
    return result
end

function VKM.set_cluster_flags(flags)
    local ctx = VKM.ctx()
    return C.ValkeyModule_SetClusterFlags(ctx, flags)
end

function VKM.cluster_key_slot_c(key)
    return tonumber(C.ValkeyModule_ClusterKeySlotC(key, #key))
end

function VKM.cluster_key_slot(key)
    return tonumber(C.ValkeyModule_ClusterKeySlot(key))
end

function VKM.cluster_canonical_key_name_in_slot(slot)
    local ptr = C.ValkeyModule_ClusterCanonicalKeyNameInSlot(slot)
    if ptr == nil then return nil end
    return ffi.string(ptr)
end

function VKM.get_client_id()
    local ctx = VKM.ctx()
    return tonumber(C.ValkeyModule_GetClientId(ctx))
end

function VKM.must_obey_client()
    local ctx = VKM.ctx()
    return C.ValkeyModule_MustObeyClient(ctx)
end

function VKM.get_client_info_by_id(id)
    local ci = ffi.new("ValkeyModuleClientInfo[1]")
    ci[0].version = 1
    local rc = C.ValkeyModule_GetClientInfoById(ci, id)
    if rc ~= VKM.OK then
        return nil
    end
    return {
        id = tonumber(ci[0].id),
        addr = ffi.string(ci[0].addr),
        port = ci[0].port,
        db = ci[0].db,
        flags = tonumber(ci[0].flags)
    }
end

function VKM.get_client_name_by_id(id)
    local ctx = VKM.ctx()
    return C.ValkeyModule_GetClientNameById(ctx, id)
end

function VKM.set_client_name_by_id(id, name)
    local ctx = VKM.ctx()
    if ctx == nil then return VKM.ERR end
    if type(name) == "string" then
        local rms = C.ValkeyModule_CreateString(ctx, name, #name)
        local rc = C.ValkeyModule_SetClientNameById(id, rms)
        C.ValkeyModule_FreeString(ctx, rms)
        return rc
    else
        return C.ValkeyModule_SetClientNameById(id, name)
    end
end

function VKM.get_client_username_by_id(id)
    local ctx = VKM.ctx()
    return C.ValkeyModule_GetClientUserNameById(ctx, id)
end

function VKM.get_client_certificate(id)
    local ctx = VKM.ctx()
    return C.ValkeyModule_GetClientCertificate(ctx, id)
end

function VKM.redact_client_command_argument(pos)
    local ctx = VKM.ctx()
    return C.ValkeyModule_RedactClientCommandArgument(ctx, pos)
end

function VKM.callreply_proto(reply)
    local ptr = C.ValkeyModule_CallReplyProto(reply, _len)
    if ptr == nil then return nil end
    return ffi.string(ptr, _len[0])
end

function VKM.callreply_bignumber(reply)
    local ptr = C.ValkeyModule_CallReplyBigNumber(reply, _len)
    if ptr == nil then return nil end
    return ffi.string(ptr, _len[0])
end

function VKM.callreply_verbatim(reply)
    local format_ptr = ffi.new("const char *[1]")
    local ptr = C.ValkeyModule_CallReplyVerbatim(reply, _len, format_ptr)
    if ptr == nil or format_ptr[0] == nil then return nil, nil end
    local format_str = ffi.string(format_ptr[0])
    return ffi.string(ptr, _len[0]), format_str
end

function VKM.callreply_double(reply)
    return C.ValkeyModule_CallReplyDouble(reply)
end

function VKM.callreply_bool(reply)
    return C.ValkeyModule_CallReplyBool(reply)
end

function VKM.callreply_length(reply)
    return tonumber(C.ValkeyModule_CallReplyLength(reply))
end

function VKM.callreply_set_element(reply, idx)
    return C.ValkeyModule_CallReplySetElement(reply, idx)
end

function VKM.callreply_map_element(reply, idx)
    local key_ptr = ffi.new("ValkeyModuleCallReply *[1]")
    local val_ptr = ffi.new("ValkeyModuleCallReply *[1]")
    local rc = C.ValkeyModule_CallReplyMapElement(reply, idx, key_ptr, val_ptr)
    if rc ~= VKM.OK then return nil, nil end
    return key_ptr[0], val_ptr[0]
end

function VKM.callreply_attribute_element(reply, idx)
    local key_ptr = ffi.new("ValkeyModuleCallReply *[1]")
    local val_ptr = ffi.new("ValkeyModuleCallReply *[1]")
    local rc = C.ValkeyModule_CallReplyAttributeElement(reply, idx, key_ptr, val_ptr)
    if rc ~= VKM.OK then return nil, nil end
    return key_ptr[0], val_ptr[0]
end

function VKM.callreply_attribute(reply)
    return C.ValkeyModule_CallReplyAttribute(reply)
end

function VKM.callreply_promise_abort(reply)
    local private_data_ptr = ffi.new("void *[1]")
    local rc = C.ValkeyModule_CallReplyPromiseAbort(reply, private_data_ptr)
    return rc, private_data_ptr[0]
end

function VKM.create_string_from_call_reply(reply)
    local ctx = VKM.ctx()
    return C.ValkeyModule_CreateStringFromCallReply(ctx, reply)
end

function VKM.open_key(name, mode)
    local ctx = VKM.ctx()
    if type(name) == "string" then
        local rms = C.ValkeyModule_CreateString(ctx, name, #name)
        local key = C.ValkeyModule_OpenKey(ctx, rms, mode)
        C.ValkeyModule_FreeString(ctx, rms)
        return key
    else
        return C.ValkeyModule_OpenKey(ctx, name, mode)
    end
end

function VKM.key_exists(name)
    local ctx = VKM.ctx()
    if type(name) == "string" then
        local rms = C.ValkeyModule_CreateString(ctx, name, #name)
        local exists = C.ValkeyModule_KeyExists(ctx, rms)
        C.ValkeyModule_FreeString(ctx, rms)
        return exists
    else
        return C.ValkeyModule_KeyExists(ctx, name)
    end
end

function VKM.get_open_key_modes_all()
    return C.ValkeyModule_GetOpenKeyModesAll()
end

function VKM.signal_modified_key(keyname)
    local ctx = VKM.ctx()
    if type(keyname) == "string" then
        local rms = C.ValkeyModule_CreateString(ctx, keyname, #keyname)
        local rc = C.ValkeyModule_SignalModifiedKey(ctx, rms)
        C.ValkeyModule_FreeString(ctx, rms)
        return rc
    else
        return C.ValkeyModule_SignalModifiedKey(ctx, keyname)
    end
end

function VKM.create_string(str)
    local ctx = VKM.ctx()
    return C.ValkeyModule_CreateString(ctx, str, #str)
end

function VKM.create_string_from_ull(ull)
    local ctx = VKM.ctx()
    return C.ValkeyModule_CreateStringFromULongLong(ctx, ull)
end

function VKM.create_string_from_ld(ld, humanfriendly)
    local ctx = VKM.ctx()
    return C.ValkeyModule_CreateStringFromLongDouble(ctx, ld, humanfriendly and 1 or 0)
end

function VKM.create_string_copy(str)
    local ctx = VKM.ctx()
    return C.ValkeyModule_CreateStringFromString(ctx, str)
end

function VKM.create_string_from_streamid(id)
    local ctx = VKM.ctx()
    return C.ValkeyModule_CreateStringFromStreamID(ctx, id)
end

function VKM.string_set(key, str)
    if type(str) == "string" then
        local ctx = VKM.ctx()
        local rms = C.ValkeyModule_CreateString(ctx, str, #str)
        local rc = C.ValkeyModule_StringSet(key, rms)
        C.ValkeyModule_FreeString(ctx, rms)
        return rc
    else
        return C.ValkeyModule_StringSet(key, str)
    end
end

function VKM.string_dma(key, mode)
    local len = ffi.new("size_t[1]")
    local ptr = C.ValkeyModule_StringDMA(key, len, mode)
    if ptr == nil then return nil, 0 end
    return ffi.cast("char *", ptr), len[0]
end

function VKM.string_truncate(key, newlen)
    return C.ValkeyModule_StringTruncate(key, newlen)
end

function VKM.value_length(key)
    return tonumber(C.ValkeyModule_ValueLength(key))
end

function VKM.string_to_double(str)
    local out = ffi.new("double[1]")
    local rc = C.ValkeyModule_StringToDouble(str, out)
    if rc == VKM.OK then
        return out[0], VKM.OK
    end
    return nil, VKM.ERR
end

function VKM.string_to_longdouble(str)
    local out = ffi.new("long double[1]")
    local rc = C.ValkeyModule_StringToLongDouble(str, out)
    if rc == VKM.OK then
        return out[0], VKM.OK
    end
    return nil, VKM.ERR
end

function VKM.string_to_ulonglong(str)
    local out = ffi.new("unsigned long long[1]")
    local rc = C.ValkeyModule_StringToULongLong(str, out)
    if rc == VKM.OK then
        return out[0], VKM.OK
    end
    return nil, VKM.ERR
end



function VKM.hash_set_string_ref(key, field, buf, len)
    return C.ValkeyModule_HashSetStringRef(key, field, buf, len)
end

function VKM.hash_has_string_ref(key, field)
    return C.ValkeyModule_HashHasStringRef(key, field)
end

function VKM.delete_key(key)
    return C.ValkeyModule_DeleteKey(key)
end

function VKM.unlink_key(key)
    return C.ValkeyModule_UnlinkKey(key)
end

function VKM.zset_first_in_lex_range(key, min, max)
    local ctx = VKM.ctx()
    local min_str = type(min) == "string" and VKM.create_string(min) or min
    local max_str = type(max) == "string" and VKM.create_string(max) or max
    local rc = C.ValkeyModule_ZsetFirstInLexRange(key, min_str, max_str)
    if type(min) == "string" then C.ValkeyModule_FreeString(ctx, min_str) end
    if type(max) == "string" and max ~= min then C.ValkeyModule_FreeString(ctx, max_str) end
    return rc
end

function VKM.zset_last_in_lex_range(key, min, max)
    local ctx = VKM.ctx()
    local min_str = type(min) == "string" and VKM.create_string(min) or min
    local max_str = type(max) == "string" and VKM.create_string(max) or max
    local rc = C.ValkeyModule_ZsetLastInLexRange(key, min_str, max_str)
    if type(min) == "string" then C.ValkeyModule_FreeString(ctx, min_str) end
    if type(max) == "string" and max ~= min then C.ValkeyModule_FreeString(ctx, max_str) end
    return rc
end

function VKM.zset_score(key, ele)
    local ctx = VKM.ctx()
    local ele_str = type(ele) == "string" and VKM.create_string(ele) or ele
    local out = ffi.new("double[1]")
    local rc = C.ValkeyModule_ZsetScore(key, ele_str, out)
    if type(ele) == "string" then C.ValkeyModule_FreeString(ctx, ele_str) end
    if rc == VKM.OK then
        return out[0], VKM.OK
    end
    return nil, VKM.ERR
end

function VKM.zset_remove(key, ele)
    local ctx = VKM.ctx()
    local ele_str = type(ele) == "string" and VKM.create_string(ele) or ele
    local deleted = ffi.new("int[1]")
    local rc = C.ValkeyModule_ZsetRem(key, ele_str, deleted)
    if type(ele) == "string" then C.ValkeyModule_FreeString(ctx, ele_str) end
    return rc, deleted[0]
end

function VKM.zset_incrby(key, score, ele)
    local ctx = VKM.ctx()
    local ele_str = type(ele) == "string" and VKM.create_string(ele) or ele
    local flags = ffi.new("int[1]")
    local newscore = ffi.new("double[1]")
    local rc = C.ValkeyModule_ZsetIncrby(key, score, ele_str, flags, newscore)
    if type(ele) == "string" then C.ValkeyModule_FreeString(ctx, ele_str) end
    if rc == VKM.OK then
        return newscore[0], VKM.OK, flags[0]
    end
    return nil, VKM.ERR, nil
end

function VKM.zset_first_in_score_range(key, min, max, minex, maxex)
    return C.ValkeyModule_ZsetFirstInScoreRange(key, min, max, minex or 0, maxex or 0)
end

function VKM.zset_last_in_score_range(key, min, max, minex, maxex)
    return C.ValkeyModule_ZsetLastInScoreRange(key, min, max, minex or 0, maxex or 0)
end

function VKM.zset_range_end_reached(key)
    return C.ValkeyModule_ZsetRangeEndReached(key)
end

function VKM.stream_add(key, flags, id, fields)
    if id == nil then
        flags = bit.bor(flags or 0, VKM.STREAM_ADD_AUTOID)
    end
    local sid = id and ffi.new("ValkeyModuleStreamID[id]", id) or ffi.cast("ValkeyModuleStreamID *", nil)
    local field_count = math.floor(#fields / 2)
    local argv_ptr = ffi.new("ValkeyModuleString *[?]", field_count * 2)
    local ctx = VKM.ctx()

    for i = 0, field_count * 2 - 1 do
        local str = type(fields[i+1]) == "string" and VKM.create_string(fields[i+1]) or fields[i+1]
        argv_ptr[i] = str
    end

    local rc = C.ValkeyModule_StreamAdd(key, flags, sid, argv_ptr, field_count)

    for i = 0, field_count * 2 - 1 do
        local str = argv_ptr[i]
        if type(fields[i+1]) == "string" then
            C.ValkeyModule_FreeString(ctx, str)
        end
    end

    if rc == VKM.OK then
        if id then
            return { ms = tonumber(sid.ms), seq = tonumber(sid.seq) }, rc
        else
            return nil, rc
        end
    end
    return nil, rc
end

function VKM.stream_delete(key, id)
    local sid = ffi.new("ValkeyModuleStreamID", id)
    return C.ValkeyModule_StreamDelete(key, sid)
end

function VKM.stream_iterator_start(key, flags, start_id, end_id)
    local sid1 = start_id and ffi.new("ValkeyModuleStreamID", start_id) or ffi.cast("ValkeyModuleStreamID *", nil)
    local sid2 = end_id and ffi.new("ValkeyModuleStreamID", end_id) or ffi.cast("ValkeyModuleStreamID *", nil)
    return C.ValkeyModule_StreamIteratorStart(key, flags or 0, sid1, sid2)
end

function VKM.stream_iterator_stop(key)
    C.ValkeyModule_StreamIteratorStop(key)
end

function VKM.stream_iterator_next_id(key)
    local sid = ffi.new("ValkeyModuleStreamID[1]")
    local numfields = ffi.new("long[1]")
    local rc = C.ValkeyModule_StreamIteratorNextID(key, sid, numfields)
    if rc == VKM.OK then
        return { ms = tonumber(sid[0].ms), seq = tonumber(sid[0].seq) }, tonumber(numfields[0])
    end
    return nil, nil
end

function VKM.stream_iterator_next_field(key)
    local field_ptr = ffi.new("ValkeyModuleString *[1]")
    local value_ptr = ffi.new("ValkeyModuleString *[1]")
    local rc = C.ValkeyModule_StreamIteratorNextField(key, field_ptr, value_ptr)
    if rc == VKM.OK then
        return field_ptr[0], value_ptr[0]
    end
    return nil, nil
end

function VKM.stream_iterator_delete(key)
    return C.ValkeyModule_StreamIteratorDelete(key)
end

function VKM.stream_trim_by_length(key, flags, length)
    return C.ValkeyModule_StreamTrimByLength(key, flags or 0, length)
end

function VKM.stream_trim_by_id(key, flags, id)
    local sid = ffi.new("ValkeyModuleStreamID", id)
    return C.ValkeyModule_StreamTrimByID(key, flags or 0, sid)
end

function VKM.hold_string(str)
    local ctx = VKM.ctx()
    return C.ValkeyModule_HoldString(ctx, str)
end

function VKM.trim_string(str)
    C.ValkeyModule_TrimStringAllocation(str)
end

function VKM.string_compare(a, b)
    return C.ValkeyModule_StringCompare(a, b)
end

function VKM.get_context_flags_all()
    return C.ValkeyModule_GetContextFlagsAll()
end

function VKM.get_context_flags()
    local ctx = VKM.ctx()
    return C.ValkeyModule_GetContextFlags(ctx)
end

function VKM.get_selected_db()
    local ctx = VKM.ctx()
    return C.ValkeyModule_GetSelectedDb(ctx)
end

function VKM.select_db(dbid)
    local ctx = VKM.ctx()
    return C.ValkeyModule_SelectDb(ctx, dbid)
end

function VKM.get_server_version()
    return tonumber(C.ValkeyModule_GetServerVersion())
end

function VKM.get_server_version_string()
    local v = VKM.get_server_version()
    local major = bit.rshift(bit.band(v, 0xFF0000), 16)
    local minor = bit.rshift(bit.band(v, 0xFF00), 8)
    local patch = bit.band(v, 0xFF)
    return string.format("%d.%d.%d", major, minor, patch)
end

function VKM.get_type_method_version()
    return tonumber(C.ValkeyModule_GetTypeMethodVersion())
end

function VKM.get_server_info(ctx, section)
    local data = C.ValkeyModule_GetServerInfo(ctx, section)
    if data ~= nil then
        ffi.gc(data, function(d) C.ValkeyModule_FreeServerInfo(ctx, d) end)
    end
    return data
end

function VKM.server_info_get_field_c(data, field)
    local ptr = C.ValkeyModule_ServerInfoGetFieldC(data, field)
    if ptr == nil then return nil end
    return ffi.string(ptr)
end

function VKM.server_info_get_field_signed(data, field)
    local err = ffi.new("int[1]")
    local value = C.ValkeyModule_ServerInfoGetFieldSigned(data, field, err)
    if err[0] == VKM.OK then
        return tonumber(value), VKM.OK
    end
    return nil, VKM.ERR
end

function VKM.server_info_get_field_unsigned(data, field)
    local err = ffi.new("int[1]")
    local value = C.ValkeyModule_ServerInfoGetFieldUnsigned(data, field, err)
    if err[0] == VKM.OK then
        return tonumber(value), VKM.OK
    end
    return nil, VKM.ERR
end

function VKM.server_info_get_field_double(data, field)
    local err = ffi.new("int[1]")
    local value = C.ValkeyModule_ServerInfoGetFieldDouble(data, field, err)
    if err[0] == VKM.OK then
        return tonumber(value), VKM.OK
    end
    return nil, VKM.ERR
end

function VKM.free_server_info(ctx, data)
    C.ValkeyModule_FreeServerInfo(ctx, data)
end

function VKM.get_random_bytes(len)
    local buf = ffi.new("unsigned char[?]", len)
    C.ValkeyModule_GetRandomBytes(buf, len)
    return ffi.string(buf, len)
end

function VKM.get_random_hex_chars(len)
    local buf = ffi.new("char[?]", len)
    C.ValkeyModule_GetRandomHexChars(buf, len)
    return ffi.string(buf, len)
end

function VKM.avoid_replica_traffic()
    return C.ValkeyModule_AvoidReplicaTraffic()
end

function VKM.yield(flags, busy_reply)
    local ctx = VKM.ctx()
    local reply = busy_reply or "BUSY"
    C.ValkeyModule_Yield(ctx, flags, reply)
end

function VKM.latency_add_sample(event, latency)
    C.ValkeyModule_LatencyAddSample(event, latency)
end

function VKM.alloc(bytes)
    return C.ValkeyModule_Alloc(bytes)
end

function VKM.free(ptr)
    C.ValkeyModule_Free(ptr)
end

function VKM.calloc(nmemb, size)
    return C.ValkeyModule_Calloc(nmemb, size)
end

function VKM.realloc(ptr, bytes)
    return C.ValkeyModule_Realloc(ptr, bytes)
end

function VKM.strdup(str)
    local ptr = C.ValkeyModule_Strdup(str)
    return ffi.cast("char *", ptr)
end

function VKM.get_used_memory_ratio()
    return tonumber(C.ValkeyModule_GetUsedMemoryRatio())
end

function VKM.malloc_size(ptr)
    return tonumber(C.ValkeyModule_MallocSize(ptr))
end

function VKM.malloc_usable_size(ptr)
    return tonumber(C.ValkeyModule_MallocUsableSize(ptr))
end

function VKM.malloc_size_string(str)
    return tonumber(C.ValkeyModule_MallocSizeString(str))
end

function VKM.malloc_size_dict(dict)
    return tonumber(C.ValkeyModule_MallocSizeDict(dict))
end

function VKM.microseconds()
    return tonumber(C.ValkeyModule_Microseconds())
end

function VKM.cached_microseconds()
    return tonumber(C.ValkeyModule_CachedMicroseconds())
end

function VKM.create_module_user(name)
    return C.ValkeyModule_CreateModuleUser(name)
end

function VKM.free_module_user(user)
    C.ValkeyModule_FreeModuleUser(user)
end

function VKM.set_context_user(ctx, user)
    C.ValkeyModule_SetContextUser(ctx, user)
end

function VKM.get_module_user_acl_string(user)
    return C.ValkeyModule_GetModuleUserACLString(user)
end

function VKM.get_current_user_name()
    local ctx = VKM.ctx()
    return C.ValkeyModule_GetCurrentUserName(ctx)
end

function VKM.get_module_user_from_username(name)
    if type(name) == "string" then
        local ctx = VKM.ctx()
        local rms = C.ValkeyModule_CreateString(ctx, name, #name)
        local user = C.ValkeyModule_GetModuleUserFromUserName(rms)
        C.ValkeyModule_FreeString(ctx, rms)
        return user
    else
        return C.ValkeyModule_GetModuleUserFromUserName(name)
    end
end

function VKM.acl_check_command_permissions(user, argv, argc)
    return C.ValkeyModule_ACLCheckCommandPermissions(user, argv, argc)
end

function VKM.acl_check_key_permissions(user, key, flags)
    return C.ValkeyModule_ACLCheckKeyPermissions(user, key, flags)
end

function VKM.acl_check_channel_permissions(user, ch, literal)
    return C.ValkeyModule_ACLCheckChannelPermissions(user, ch, literal)
end

function VKM.acl_check_permissions(user, argv, argc, dbid)
    local reason = ffi.new("ValkeyModuleACLLogEntryReason[1]")
    local rc = C.ValkeyModule_ACLCheckPermissions(user, argv, argc, dbid, reason)
    if rc ~= VKM.OK then
        return rc, reason[0]
    end
    return rc, nil
end

function VKM.acl_add_log_entry(user, object, reason)
    local ctx = VKM.ctx()
    C.ValkeyModule_ACLAddLogEntry(ctx, user, object, reason)
end

function VKM.acl_add_log_entry_by_username(username, object, reason)
    local ctx = VKM.ctx()
    C.ValkeyModule_ACLAddLogEntryByUserName(ctx, username, object, reason)
end

function VKM.deauthenticate_and_close_client(client_id)
    local ctx = VKM.ctx()
    return C.ValkeyModule_DeauthenticateAndCloseClient(ctx, client_id)
end

function VKM.acl_check_key_prefix_permissions(user, key, len, flags)
    return C.ValkeyModule_ACLCheckKeyPrefixPermissions(user, key, len, flags)
end

local function rms_to_lua_string(rms)
    local ptr = C.ValkeyModule_StringPtrLen(rms, _len)
    if ptr == nil then return nil end
    return ffi.string(ptr, _len[0])
end

local VMString_methods = {}
local VMString_mt = { __index = VMString_methods }

function VMString_methods:PtrLen()
    local ptr = C.ValkeyModule_StringPtrLen(self, _len)
    return ptr, _len[0]
end

function VMString_methods:ToLongLong()
    local out = ffi.new("long long[1]")
    local rc = C.ValkeyModule_StringToLongLong(self, out)
    if rc == 0 then return tonumber(out[0]), rc end
    return nil, rc
end

function VMString_methods:ToDouble()
    local out = ffi.new("double[1]")
    local rc = C.ValkeyModule_StringToDouble(self, out)
    if rc == 0 then return tonumber(out[0]), rc end
    return nil, rc
end

function VMString_methods:ToULongLong()
    local out = ffi.new("unsigned long long[1]")
    local rc = C.ValkeyModule_StringToULongLong(self, out)
    if rc == 0 then return tonumber(out[0]), rc end
    return nil, rc
end

function VMString_methods:ToLongDouble()
    local out = ffi.new("long double[1]")
    local rc = C.ValkeyModule_StringToLongDouble(self, out)
    if rc == 0 then return tonumber(out[0]), rc end
    return nil, rc
end

function VMString_methods:ToStreamID()
    local out = ffi.new("ValkeyModuleStreamID[1]")
    local rc = C.ValkeyModule_StringToStreamID(self, out)
    if rc == 0 then
        return { ms = tonumber(out[0].ms), seq = tonumber(out[0].seq) }, rc
    end
    return nil, rc
end

function VMString_methods:Compare(other)
    return C.ValkeyModule_StringCompare(self, other)
end

function VMString_methods:Trim()
    C.ValkeyModule_TrimStringAllocation(self)
end

function VMString_methods:Copy()
    local ctx = VKM.ctx()
    return C.ValkeyModule_CreateStringFromString(ctx, self)
end

function VMString_methods:Hold()
    local ctx = VKM.ctx()
    return C.ValkeyModule_HoldString(ctx, self)
end

VMString_mt.__tostring = rms_to_lua_string

ffi.metatype("ValkeyModuleString", VMString_mt)

local VMKey_methods = {}
local VMKey_mt = { __index = VMKey_methods }

function VMKey_methods:Close()
    C.ValkeyModule_CloseKey(self)
end

function VMKey_methods:KeyType()
    return C.ValkeyModule_KeyType(self)
end

function VMKey_methods:ValueLength()
    return C.ValkeyModule_ValueLength(self)
end

function VMKey_methods:StringSet(str)
    return C.ValkeyModule_StringSet(self, str)
end

function VMKey_methods:ListPush(where, ele)
    return C.ValkeyModule_ListPush(self, where, ele)
end

function VMKey_methods:ListPop(where)
    return C.ValkeyModule_ListPop(self, where)
end

function VMKey_methods:ListGet(index)
    return C.ValkeyModule_ListGet(self, index)
end

function VMKey_methods:ListSet(index, value)
    return C.ValkeyModule_ListSet(self, index, value)
end

function VMKey_methods:ListInsert(index, value)
    return C.ValkeyModule_ListInsert(self, index, value)
end

function VMKey_methods:ListDelete(index)
    return C.ValkeyModule_ListDelete(self, index)
end

function VMKey_methods:GetExpire()
    return tonumber(C.ValkeyModule_GetExpire(self))
end

function VMKey_methods:SetExpire(expire)
    return C.ValkeyModule_SetExpire(self, expire)
end

function VMKey_methods:GetAbsExpire()
    return tonumber(C.ValkeyModule_GetAbsExpire(self))
end

function VMKey_methods:SetAbsExpire(expire)
    return C.ValkeyModule_SetAbsExpire(self, expire)
end

function VMKey_methods:Unlink()
    return C.ValkeyModule_UnlinkKey(self)
end

function VMKey_methods:HashSetStringRef(field, buf, len)
    return C.ValkeyModule_HashSetStringRef(self, field, buf, len)
end

function VMKey_methods:HashHasStringRef(field)
    return C.ValkeyModule_HashHasStringRef(self, field)
end

function VMKey_methods:ZsetFirstInLexRange(min, max)
    return VKM.zset_first_in_lex_range(self, min, max)
end

function VMKey_methods:ZsetLastInLexRange(min, max)
    return VKM.zset_last_in_lex_range(self, min, max)
end

function VMKey_methods:StreamAdd(flags, id, fields)
    return VKM.stream_add(self, flags or 0, id, fields)
end

function VMKey_methods:StreamDelete(id)
    return VKM.stream_delete(self, id)
end

function VMKey_methods:StreamIteratorStart(flags, start_id, end_id)
    return VKM.stream_iterator_start(self, flags or 0, start_id, end_id)
end

function VMKey_methods:StreamIteratorStop()
    return VKM.stream_iterator_stop(self)
end

function VMKey_methods:StreamIteratorNextID()
    return VKM.stream_iterator_next_id(self)
end

function VMKey_methods:StreamIteratorNextField()
    return VKM.stream_iterator_next_field(self)
end

function VMKey_methods:StreamIteratorDelete()
    return VKM.stream_iterator_delete(self)
end

function VMKey_methods:StreamTrimByLength(flags, length)
    return VKM.stream_trim_by_length(self, flags or 0, length)
end

function VMKey_methods:StreamTrimByID(flags, id)
    return VKM.stream_trim_by_id(self, flags or 0, id)
end

function VMKey_methods:Delete()
    return C.ValkeyModule_DeleteKey(self)
end

function VMKey_methods:StringDMA(mode)
    local len_ptr = ffi.new("size_t[1]")
    local ptr = C.ValkeyModule_StringDMA(self, len_ptr, mode)
    if ptr == nil then return nil, nil end
    return ptr, len_ptr[0]
end

function VMKey_methods:StringTruncate(newlen)
    return C.ValkeyModule_StringTruncate(self, newlen)
end

function VMKey_methods:ZsetFirstInScoreRange(min, max, minex, maxex)
    return C.ValkeyModule_ZsetFirstInScoreRange(self, min, max, minex and 1 or 0, maxex and 1 or 0)
end

function VMKey_methods:ZsetLastInScoreRange(min, max, minex, maxex)
    return C.ValkeyModule_ZsetLastInScoreRange(self, min, max, minex and 1 or 0, maxex and 1 or 0)
end

function VMKey_methods:ZsetRangeNext()
    return C.ValkeyModule_ZsetRangeNext(self)
end

function VMKey_methods:ZsetRangePrev()
    return C.ValkeyModule_ZsetRangePrev(self)
end

function VMKey_methods:ZsetRangeEndReached()
    return C.ValkeyModule_ZsetRangeEndReached(self)
end

function VMKey_methods:ZsetRangeCurrentElement()
    local score_ptr = ffi.new("double[1]")
    local elem = C.ValkeyModule_ZsetRangeCurrentElement(self, score_ptr)
    if elem == nil then return nil, nil end
    return elem, tonumber(score_ptr[0])
end

function VMKey_methods:ZsetRangeStop()
    C.ValkeyModule_ZsetRangeStop(self)
end

ffi.metatype("ValkeyModuleKey", VMKey_mt)

local VMServerInfoData_mt = {}

function VMServerInfoData_mt.__index(data, field)
    local ptr = C.ValkeyModule_ServerInfoGetFieldC(data, field)
    if ptr == nil then return nil end
    return ffi.string(ptr)
end

ffi.metatype("ValkeyModuleServerInfoData", VMServerInfoData_mt)

VKM.CMD_GETKEYS_POS_INDEX = 0
VKM.CMD_GETKEYS_POS_CHANNEL = 1

VKM.CMD_CHANNEL_PATTERN = 0x00000001
VKM.CMD_CHANNEL_PUBLISH = 0x00000002
VKM.CMD_CHANNEL_SUBSCRIBE = 0x00000004
VKM.CMD_CHANNEL_UNSUBSCRIBE = 0x00000008

function VKM.is_keys_position_request()
    local ctx = VKM.ctx()
    return C.ValkeyModule_IsKeysPositionRequest(ctx)
end

function VKM.key_at_pos(pos)
    local ctx = VKM.ctx()
    C.ValkeyModule_KeyAtPos(ctx, pos)
end

function VKM.key_at_pos_with_flags(pos, flags)
    local ctx = VKM.ctx()
    C.ValkeyModule_KeyAtPosWithFlags(ctx, pos, flags)
end

function VKM.is_channels_position_request()
    local ctx = VKM.ctx()
    return C.ValkeyModule_IsChannelsPositionRequest(ctx)
end

function VKM.channel_at_pos_with_flags(pos, flags)
    local ctx = VKM.ctx()
    C.ValkeyModule_ChannelAtPosWithFlags(ctx, pos, flags)
end

function VKM.string_append_buffer(str, buf)
    local ctx = VKM.ctx()
    return C.ValkeyModule_StringAppendBuffer(ctx, str, buf, #buf)
end

function VKM.retain_string(str)
    local ctx = VKM.ctx()
    C.ValkeyModule_RetainString(ctx, str)
end

function VKM.create_string_printf(fmt, ...)
    local ctx = VKM.ctx()
    return C.ValkeyModule_CreateStringPrintf(ctx, fmt, ...)
end

VKM.CONFIG_DEFAULT = 0
VKM.CONFIG_IMMUTABLE = 0x0001
VKM.CONFIG_SENSITIVE = 0x0002
VKM.CONFIG_HIDDEN = 0x0010
VKM.CONFIG_PROTECTED = 0x0020
VKM.CONFIG_DENY_LOADING = 0x0040
VKM.CONFIG_MEMORY = 0x0080
VKM.CONFIG_BITFLAGS = 0x0100
VKM.CONFIG_UNSIGNED = 0x0200

function VKM.get_shared_api(apiname)
    local ctx = VKM.ctx()
    return C.ValkeyModule_GetSharedAPI(ctx, apiname)
end

function VKM.milliseconds()
    return tonumber(C.ValkeyModule_Milliseconds())
end

function VKM.monotonic_microseconds()
    return tonumber(C.ValkeyModule_MonotonicMicroseconds())
end

function VKM.try_alloc(bytes)
    return C.ValkeyModule_TryAlloc(bytes)
end

function VKM.try_calloc(nmemb, size)
    return C.ValkeyModule_TryCalloc(nmemb, size)
end

function VKM.try_realloc(ptr, bytes)
    return C.ValkeyModule_TryRealloc(ptr, bytes)
end

function VKM.pool_alloc(bytes)
    local ctx = VKM.ctx()
    return C.ValkeyModule_PoolAlloc(ctx, bytes)
end

function VKM.is_aof_client()
    local client_id = VKM.get_client_id()
    return client_id == 0xFFFFFFFFFFFFFFFFULL
end

VKM.UINT64_MAX = 0xFFFFFFFFFFFFFFFFULL

function VKM.add_acl_category(name)
    local ctx = VKM.ctx()
    return C.ValkeyModule_AddACLCategory(ctx, name)
end

function VKM.notify_keyspace_event(event_type, event, key)
    local ctx = VKM.ctx()
    if type(key) == "string" then
        local rms = C.ValkeyModule_CreateString(ctx, key, #key)
        C.ValkeyModule_NotifyKeyspaceEvent(ctx, event_type, event, rms)
        C.ValkeyModule_FreeString(ctx, rms)
    else
        C.ValkeyModule_NotifyKeyspaceEvent(ctx, event_type, event, key)
    end
end

function VKM.subscribe_to_keyspace_events(types)
    local ctx = VKM.ctx()
    return C.ValkeyModule_SubscribeToKeyspaceEvents(ctx, types)
end

function VKM.get_notify_keyspace_events()
    return C.ValkeyModule_GetNotifyKeyspaceEvents()
end

function VKM.get_keyspace_notification_flags_all()
    return C.ValkeyModule_GetKeyspaceNotificationFlagsAll()
end

function VKM.is_sub_event_supported(event, subevent)
    return C.ValkeyModule_IsSubEventSupported(event, subevent)
end

function VKM.get_thread_safe_context(ctx)
    return C.ValkeyModule_GetThreadSafeContext(ctx or nil)
end

function VKM.get_detached_thread_safe_context(ctx)
    return C.ValkeyModule_GetDetachedThreadSafeContext(ctx or nil)
end

function VKM.free_thread_safe_context(ctx)
    C.ValkeyModule_FreeThreadSafeContext(ctx)
end

function VKM.thread_safe_context_lock(ctx)
    C.ValkeyModule_ThreadSafeContextLock(ctx)
end

function VKM.thread_safe_context_trylock(ctx)
    return C.ValkeyModule_ThreadSafeContextTryLock(ctx)
end

function VKM.thread_safe_context_unlock(ctx)
    C.ValkeyModule_ThreadSafeContextUnlock(ctx)
end

function VKM.create_dict(ctx)
    return C.ValkeyModule_CreateDict(ctx or nil)
end

function VKM.free_dict(ctx, d)
    C.ValkeyModule_FreeDict(ctx or nil, d)
end

function VKM.dict_size(d)
    return tonumber(C.ValkeyModule_DictSize(d))
end

function VKM.dict_set_c(d, key, keylen, ptr)
    return C.ValkeyModule_DictSetC(d, key, keylen, ptr)
end

function VKM.dict_replace_c(d, key, keylen, ptr)
    return C.ValkeyModule_DictReplaceC(d, key, keylen, ptr)
end

function VKM.dict_set(d, key, ptr)
    return C.ValkeyModule_DictSet(d, key, ptr)
end

function VKM.dict_replace(d, key, ptr)
    return C.ValkeyModule_DictReplace(d, key, ptr)
end

function VKM.dict_get_c(d, key, keylen)
    local nokey = ffi.new("int[1]")
    local ptr = C.ValkeyModule_DictGetC(d, key, keylen, nokey)
    return (nokey[0] == 0) and ptr or nil
end

function VKM.dict_get(d, key)
    local nokey = ffi.new("int[1]")
    local ptr = C.ValkeyModule_DictGet(d, key, nokey)
    return (nokey[0] == 0) and ptr or nil
end

function VKM.dict_del_c(d, key, keylen)
    return C.ValkeyModule_DictDelC(d, key, keylen, nil)
end

function VKM.dict_del(d, key)
    return C.ValkeyModule_DictDel(d, key, nil)
end

function VKM.dict_iterator_start_c(d, op, key, keylen)
    return C.ValkeyModule_DictIteratorStartC(d, op or "^", key, keylen or 0)
end

function VKM.dict_iterator_start(d, op, key)
    return C.ValkeyModule_DictIteratorStart(d, op or "^", key)
end

function VKM.dict_iterator_stop(di)
    C.ValkeyModule_DictIteratorStop(di)
end

function VKM.dict_next_c(di)
    local keylen = ffi.new("size_t[1]")
    local ptr = C.ValkeyModule_DictNextC(di, keylen, nil)
    if ptr == nil then return nil, nil end
    return ptr, keylen[0]
end

function VKM.dict_next(di)
    local key_ptr = ffi.new("ValkeyModuleString *[1]")
    local ptr = C.ValkeyModule_DictNext(di, key_ptr, nil)
    if ptr == nil then return nil, nil end
    return ptr, key_ptr[0]
end

function VKM.dict_prev_c(di)
    local keylen = ffi.new("size_t[1]")
    local ptr = C.ValkeyModule_DictPrevC(di, keylen, nil)
    if ptr == nil then return nil, nil end
    return ptr, keylen[0]
end

function VKM.dict_prev(di)
    local key_ptr = ffi.new("ValkeyModuleString *[1]")
    local ptr = C.ValkeyModule_DictPrev(di, key_ptr, nil)
    if ptr == nil then return nil, nil end
    return ptr, key_ptr[0]
end

function VKM.dict_compare_c(d, key1, keylen1, key2, keylen2)
    return C.ValkeyModule_DictCompareC(d, key1, keylen1, key2, keylen2)
end

function VKM.dict_compare(d, key1, key2)
    return C.ValkeyModule_DictCompare(d, key1, key2)
end

function VKM.dict_iterator_reseek_c(di, key, keylen)
    C.ValkeyModule_DictIteratorReseekC(di, key, keylen)
end

function VKM.dict_iterator_reseek(di, key)
    C.ValkeyModule_DictIteratorReseek(di, key)
end

function VKM.create_string_from_buffer(ptr, len)
    local ctx = VKM.ctx()
    local buffer = ffi.string(ptr, len)
    return C.ValkeyModule_CreateString(ctx, buffer, len)
end

function VKM.get_lru(key)
    local lru_idle = ffi.new("mstime_t[1]")
    local rc = C.ValkeyModule_GetLRU(key, lru_idle)
    return tonumber(lru_idle[0])
end

function VKM.set_lru(key, lru_idle_time_ms)
    return C.ValkeyModule_SetLRU(key, lru_idle_time_ms)
end

function VKM.get_lfu(key)
    local lfu_freq = ffi.new("long long[1]")
    local rc = C.ValkeyModule_GetLFU(key, lfu_freq)
    return tonumber(lfu_freq[0])
end

function VKM.set_lfu(key, lfu_freq)
    return C.ValkeyModule_SetLFU(key, lfu_freq)
end

function VKM.set_module_user_acl_string(ctx, user, acl)
    local err_ptr = ffi.new("ValkeyModuleString *[1]")
    local rc = C.ValkeyModule_SetModuleUserACLString(ctx, user, acl, err_ptr)
    if rc ~= VKM.OK and err_ptr[0] ~= nil then
        C.ValkeyModule_FreeString(ctx, err_ptr[0])
    end
    return rc
end

function VKM.set_module_user_acl(user, acl)
    return C.ValkeyModule_SetModuleUserACL(user, acl)
end

VKM.C = C

return VKM
