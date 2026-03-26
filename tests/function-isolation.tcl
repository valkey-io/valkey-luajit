proc get_function_code {engine library_name function_name body} {
    return [format "#!%s name=%s\nserver.register_function('%s', function(KEYS, ARGV)\n %s \nend)" $engine $library_name $function_name $body]
}

start_server {tags {"function-isolation"}} {
    test {Users are created with scripting permissions} {
        r ACL SETUSER user1 on >pass1 +@scripting ~*
        r ACL SETUSER user2 on >pass2 +@scripting ~*
    }

    set u1 [valkey [srv "host"] [srv "port"] 0 $::tls]
    set u2 [valkey [srv "host"] [srv "port"] 0 $::tls]

    $u1 AUTH user1 pass1
    $u2 AUTH user2 pass2

    test {Global variable set by user1 is not visible to user2 via FCALL} {
        r FUNCTION LOAD [get_function_code lua lib1 set_foo {foo = 'bar'; return 'set'}]
        r FUNCTION LOAD [get_function_code lua lib2 get_foo {return foo or ''}]

        $u1 FCALL set_foo 0

        assert_equal [$u1 FCALL get_foo 0] {bar}

        assert_equal [$u2 FCALL get_foo 0] {}
    }

    test {Global variable set by user2 is not visible to user1 via FCALL} {
        r FUNCTION LOAD [get_function_code lua lib3 set_baz {baz = 'test'; return 'set'}]
        r FUNCTION LOAD [get_function_code lua lib4 get_baz {return baz or ''}]

        $u2 FCALL set_baz 0

        assert_equal [$u2 FCALL get_baz 0] {test}

        assert_equal [$u1 FCALL get_baz 0] {}
    }

    test {Both users have independent global state via FCALL} {
        r FUNCTION LOAD [get_function_code lua lib5 set_value {foo = ARGV[1]; return 'set'}]
        r FUNCTION LOAD [get_function_code lua lib6 get_value {return foo or ''}]

        $u1 FCALL set_value 0 updated
        $u2 FCALL set_value 0 different

        assert_equal [$u1 FCALL get_value 0] {updated}

        assert_equal [$u2 FCALL get_value 0] {different}
    }

    test {user1 can create function with function load and user2 can call it with fcall} {
        r FUNCTION LOAD [get_function_code lua lib7 hello {return 'hello from lib'}]

        assert_equal [$u1 FCALL hello 0] {hello from lib}
        assert_equal [$u2 FCALL hello 0] {hello from lib}
    }

    test {New user is able to call that function too} {
        r ACL SETUSER user3 on >pass3 +@scripting ~*
        set u3 [valkey [srv "host"] [srv "port"] 0 $::tls]
        $u3 AUTH user3 pass3

        assert_equal [$u3 FCALL hello 0] {hello from lib}

        $u3 close
    }

    $u1 close
    $u2 close
    r ACL DELUSER user1 user2 user3
}
