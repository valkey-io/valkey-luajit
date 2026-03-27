start_server {tags {"multi-engine"} overrides {luajit.enable-ffi-api yes luajit.engine-name "lua,luajit,foobar"}} {
    test {Engine registered with 'lua' name} {
        set result [r EVAL "#!lua\nreturn jit.version" 0]
        assert_match {LuaJIT*} $result
    }

    test {Engine registered with 'luajit' name} {
        set result [r EVAL "#!luajit\nreturn jit.version" 0]
        assert_match {LuaJIT*} $result
    }

    test {Engine registered with 'foobar' name} {
        set result [r EVAL "#!foobar\nreturn jit.version" 0]
        assert_match {LuaJIT*} $result
    }

    test {SCRIPT LOAD and EVALSHA with 'lua' engine} {
        set sha [r SCRIPT LOAD "#!lua\nreturn 'test-lua'"]
        set result [r EVALSHA $sha 0]
        assert_equal $result {test-lua}
    }

    test {SCRIPT LOAD and EVALSHA with 'luajit' engine} {
        set sha [r SCRIPT LOAD "#!luajit\nreturn 'test-luajit'"]
        set result [r EVALSHA $sha 0]
        assert_equal $result {test-luajit}
    }

    test {SCRIPT LOAD and EVALSHA with 'foobar' engine} {
        set sha [r SCRIPT LOAD "#!foobar\nreturn 'test-foobar'"]
        set result [r EVALSHA $sha 0]
        assert_equal $result {test-foobar}
    }

    test {'lua' 'luajit' 'foobar' point to same engine} {
        set lua_v [r EVAL "#!lua\nreturn jit.version" 0]
        set luajit_v [r EVAL "#!luajit\nreturn jit.version" 0]
        set foobar_v [r EVAL "#!foobar\nreturn jit.version" 0]
        assert_equal $lua_v $luajit_v
        assert_equal $luajit_v $foobar_v
    }
}
