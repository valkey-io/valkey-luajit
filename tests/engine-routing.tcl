proc get_function_code {engine library_name function_name body} {
    return [format "#!%s name=%s\nserver.register_function('%s', function(KEYS, ARGV)\n %s \nend)" $engine $library_name $function_name $body]
}

start_server {tags {"engine-coexistence"} overrides {luajit.enable-ffi-api yes luajit.engine-name "luajit"}} {
    test {EVAL uses built-in Lua engine} {
        set result [r EVAL "return _VERSION" 0]
        assert_match {*Lua 5.*} $result
    }

    test {FUNCTION with #!lua shebang uses built-in Lua} {
        r FUNCTION LOAD [get_function_code lua lib1 get_version {return _VERSION}]
        set result [r FCALL get_version 0]
        assert_match {*Lua 5.*} $result
    }

    test {FUNCTION with #!luajit shebang uses LuaJIT} {
        r SET KEY VALUE
        r FUNCTION LOAD [get_function_code luajit lib2 get_test {return VKM.random_key()}]
        set result [r FCALL get_test 0]
        assert_match {*KEY*} $result
    }
}
