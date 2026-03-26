proc get_function_code {engine library_name function_name body} {
    return [format "#!%s name=%s\nserver.register_function('%s', function(KEYS, ARGV)\n %s \nend)" $engine $library_name $function_name $body]
}

start_server {tags {"function-coexistence"} overrides {luajit.engine-name "luajit"}} {
    test {Both engines can be used simultaneously} {
        r FUNCTION LOAD [get_function_code lua simlib1 from_lua {return 'from-Lua'}]
        r FUNCTION LOAD [get_function_code luajit simlib2 from_luajit {return 'from-LuaJIT'}]

        assert_equal [r FCALL from_lua 0] {from-Lua}
        assert_equal [r FCALL from_luajit 0] {from-LuaJIT}
    }

    test {Both engines can interact with the same keys} {
        r FUNCTION LOAD [get_function_code lua writelib lua_write {return server.call('set', KEYS[1], ARGV[1])}]
        r FUNCTION LOAD [get_function_code luajit readlib luajit_read {return server.call('get', KEYS[1])}]

        r FCALL lua_write 1 mykey hello
        assert_equal [r FCALL luajit_read 1 mykey] {hello}
    }

    test {LuaJIT function can read data written by built-in Lua function} {
        r FUNCTION LOAD [get_function_code lua wlib2 lua_set {return server.call('set', KEYS[1], ARGV[1])}]
        r FUNCTION LOAD [get_function_code luajit rlib2 luajit_get {return server.call('get', KEYS[1])}]

        r FCALL lua_set 1 crosskey crossvalue
        assert_equal [r FCALL luajit_get 1 crosskey] {crossvalue}
    }

    test {Built-in Lua function can read data written by LuaJIT function} {
        r FUNCTION LOAD [get_function_code luajit wlib3 luajit_set {return server.call('set', KEYS[1], ARGV[1])}]
        r FUNCTION LOAD [get_function_code lua rlib3 lua_get {return server.call('get', KEYS[1])}]

        r FCALL luajit_set 1 crosskey2 crossvalue2
        assert_equal [r FCALL lua_get 1 crosskey2] {crossvalue2}
    }

    test {Error in one engine doesn't affect the other} {
        r FUNCTION LOAD [get_function_code lua oklib lua_ok {return 'lua-ok'}]
        r FUNCTION LOAD [get_function_code luajit errlib luajit_err {return server.call('invalid_command')}]
        r FUNCTION LOAD [get_function_code luajit oklib2 luajit_ok {return 'luajit-ok'}]

        catch {r FCALL luajit_err 0} e
        assert_match {*ERR*} $e

        assert_equal [r FCALL lua_ok 0] {lua-ok}
        assert_equal [r FCALL luajit_ok 0] {luajit-ok}
    }

    test {Both engines can use cjson} {
        r FUNCTION LOAD [get_function_code lua jsonlib1 lua_json {return cjson.encode({a = 1})}]
        r FUNCTION LOAD [get_function_code luajit jsonlib2 luajit_json {return cjson.encode({a = 1})}]

        set lua_result [r FCALL lua_json 0]
        set luajit_result [r FCALL luajit_json 0]

        assert_match {*"a":1*} $lua_result
        assert_match {*"a":1*} $luajit_result
    }
}
