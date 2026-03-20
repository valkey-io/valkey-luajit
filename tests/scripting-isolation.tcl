start_server {tags {"scripting-isolation"}} {
    test {Users are created with scripting permissions} {
        r ACL SETUSER user1 on >pass1 +@scripting ~*
        r ACL SETUSER user2 on >pass2 +@scripting ~*
    }

    set u1 [valkey [srv "host"] [srv "port"] 0 $::tls]
    set u2 [valkey [srv "host"] [srv "port"] 0 $::tls]

    $u1 AUTH user1 pass1
    $u2 AUTH user2 pass2

    test {Global variable set by user1 is not visible to user2} {
        $u1 EVAL "foo = 'bar'; return 'set'" 0

        assert_equal [$u1 EVAL "return foo" 0] {bar}

        assert_equal [$u2 EVAL "return foo" 0] {}
    }

    test {Global variable set by user2 is not visible to user1} {
        $u2 EVAL "baz = 'test'; return 'set'" 0

        assert_equal [$u2 EVAL "return baz" 0] {test}

        assert_equal [$u1 EVAL "return baz" 0] {}
    }

    test {Both users have independent global state} {
        $u1 EVAL "foo = 'updated'; return 'set'" 0
        $u2 EVAL "foo = 'different'; return 'set'" 0

        assert_equal [$u1 EVAL "return foo" 0] {updated}

        assert_equal [$u2 EVAL "return foo" 0] {different}
    }

    test {user1 can create script with script load and user2 can call it with evalsha} {
        set sha [$u1 SCRIPT LOAD {return 'hello from user1'}]

        assert_equal [$u2 EVALSHA $sha 0] {hello from user1}
    }

    test {New user is able to call that script too} {
        r ACL SETUSER user3 on >pass3 +@scripting ~*
        set u3 [valkey [srv "host"] [srv "port"] 0 $::tls]
        $u3 AUTH user3 pass3

        assert_equal [$u3 EVALSHA $sha 0] {hello from user1}

        $u3 close
    }

    $u1 close
    $u2 close
    r ACL DELUSER user1 user2 user3
}
