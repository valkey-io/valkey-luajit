start_server {tags {"scripting"}} {
    set brief_coroutines {
        local function spin() for i=1,200000 do end end
        local function mid() for i=1,50 do coroutine.resume(coroutine.create(spin)) end end
        for i=1,50 do coroutine.resume(coroutine.create(mid)) end
    }

    test {Timed out script of brief coroutines is detected and can be killed} {
        r config set lua-time-limit 10
        set rd [valkey_deferring_client]
        $rd eval $brief_coroutines 0

        wait_for_condition 100 10 {
            [catch {r ping} e] == 1
        } else {
            fail "Can't wait for script to start running"
        }
        assert_match {BUSY*} $e

        r script kill
        assert_error {*killed by user*} {$rd read}
        assert_equal [r ping] {PONG}
        $rd close
    }

    test {Timed out function of brief coroutines is detected and can be killed} {
        r config set lua-time-limit 10
        r function load replace [format "#!lua name=brief\nserver.register_function('brief', function() %s end)" $brief_coroutines]
        set rd [valkey_deferring_client]
        $rd fcall brief 0

        wait_for_condition 100 10 {
            [catch {r ping} e] == 1
        } else {
            fail "Can't wait for function to start running"
        }
        assert_match {BUSY*} $e

        r function kill
        assert_error {*killed by user*} {$rd read}
        assert_equal [r ping] {PONG}
        $rd close
    }
}
