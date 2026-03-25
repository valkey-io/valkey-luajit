start_server {tags {"scripting-ffi"}} {
    test {FFI - Not accessible by default} {
        r eval "local ffi = ffi; if ffi == nil then return 'not accessible' end" 0
    } {not accessible}

    test {FFI - Global VKM not accessible when FFI disabled} {
        r eval "local VKM = VKM; if VKM == nil then return 'not accessible' end" 0
    } {not accessible}

    test {FFI - Runtime config set fails (immutable)} {
        catch {r config set luajit.enable-ffi-api yes} e
        assert_match {*can't set immutable config*} $e

        r eval "local ffi = ffi; if ffi == nil then return 'not accessible' end" 0
    } {not accessible}
}

start_server {tags {"scripting-ffi"} overrides {luajit.enable-ffi-api yes}} {
    test {FFI - Accessible when enabled in config} {
        r eval "local ffi = ffi; return type(ffi)" 0
    } {table}

    test {FFI - VKM wrapper available} {
        r eval "
            local C = VKM.C
            local ctx = VKM.ctx()
            return 'VKM accessible'
        " 0
    } {VKM accessible}

    test {FFI - VKM constants accessible} {
        r eval "
            -- Verify VKM constants are accessible
            local read_val = VKM.READ
            local write_val = VKM.WRITE
            return {read_val, write_val}
        " 0
    } {1 2}

    test {FFI - random_key returns Lua string} {
        r set "key1" "value1"
        r set "key2" "value2"
        r eval "
            local key = VKM.random_key()
            return type(key)
        " 0
    } {string}
    r del "key1" "key2"

    test {FFI - random_key returns nil for empty DB} {
        r eval "
            local key = VKM.random_key()
            if key == nil then
                return 'nil'
            else
                return type(key)
            end
        " 0
    } {nil}

    test {FFI - random_key returns valid key from DB} {
        r set "testkey" "testvalue"
        r eval "
            local key = VKM.random_key()
            return key
        " 0
    } {testkey}
    r del "testkey"
}
