package flowstopper

import (
	"bytes"
	"fmt"
	"os/exec"
	"testing"
	"time"

	"github.com/WatchBeam/clock"
	"github.com/garyburd/redigo/redis"
	"github.com/rafaeljusto/redigomock"
	. "github.com/smartystreets/goconvey/convey"
)

var now = time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC)
var redisServerPort = 58789

func TestWithMockRedis(t *testing.T) {
	Convey("Given a stopper", t, func() {
		conn := redigomock.NewConn()

		stopper := Stopper{
			Namespace: "fakestopper",
			Interval:  5 * time.Second,
			Limit:     int64(5),
			ConnPool: &redis.Pool{
				Dial: func() (redis.Conn, error) {
					return conn, nil
				},
			},
			c: clock.NewMockClock(now),
		}

		multi := conn.Command("MULTI")
		exec := conn.Command("EXEC")
		zremrangebyscore := conn.Command("ZREMRANGEBYSCORE", "fakestopper:foo", "-inf", now.Add(stopper.Interval*-1).UnixNano()).Expect("QUEUED")
		zadd := conn.Command("ZADD", "fakestopper:foo", now.UnixNano(), now.UnixNano()).Expect("QUEUED")
		conn.Command("ZCARD", "fakestopper:foo").Expect("QUEUED")

		Convey("When I perform an action", func() {
			exec.Expect([]interface{}{int64(0), int64(1), int64(1)})
			passed, err := stopper.Pass("foo")

			Convey("Commands should be executed in a single transaction", func() {
				So(conn.Stats(multi), ShouldEqual, 1)
				So(conn.Stats(exec), ShouldEqual, 1)
			})

			Convey("Elements beyond the interval should be removed from the set", func() {
				So(conn.Stats(zremrangebyscore), ShouldEqual, 1)
			})

			Convey("The current nanotime is added to the set", func() {
				So(conn.Stats(zadd), ShouldEqual, 1)
			})

			Convey("The action should pass", func() {
				So(err, ShouldEqual, nil)
				So(passed, ShouldEqual, true)
			})
		})

		Convey("When I peek", func() {
			conn.Command("ZCARD", "fakestopper:foo").Expect(int64(0))
			count, err := stopper.Peek("foo")

			Convey("Count should be zero", func() {
				So(err, ShouldEqual, nil)
				So(count, ShouldEqual, 0)
			})
		})

		Convey("When the rate is exceeded", func() {
			exec.Expect([]interface{}{int64(0), int64(1), int64(6)})
			passed, err := stopper.Pass("foo")

			Convey("The action should not pass", func() {
				So(err, ShouldEqual, nil)
				So(passed, ShouldEqual, false)
			})
			Convey("When I peek", func() {
				conn.Command("ZCARD", "fakestopper:foo").Expect(int64(6))
				count, err := stopper.Peek("foo")

				Convey("Count should be 6", func() {
					So(err, ShouldEqual, nil)
					So(count, ShouldEqual, 6)
				})
			})
		})
	})
}

func TestWithRealRedis(t *testing.T) {

	redisServer := runRedisServer()
	if redisServer == nil {
		t.Fatal("redis-server didn't start")
	}
	defer func() { _ = redisServer.Process.Kill() }()

	connPool := redis.Pool{
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", fmt.Sprintf("localhost:%d", redisServerPort))
		},
	}

	flushall := func() {
		conn := connPool.Get()
		defer func() { _ = conn.Close() }()
		_, err := conn.Do("FLUSHALL")
		if err != nil {
			t.Fatal(err)
		}
	}

	Convey("Given a stopper", t, func() {
		clock := clock.NewMockClock(now)
		stopper := Stopper{
			Namespace: "realstopper",
			Interval:  5 * time.Second,
			Limit:     int64(3),
			ConnPool:  &connPool,
			c:         clock,
		}

		pass := func(item string) bool {
			clock.AddTime(1 * time.Nanosecond)
			passed, err := stopper.Pass(item)
			if err != nil {
				t.Fatal(err)
			}
			return passed
		}

		Convey("When I perform an action", func() {
			flushall()
			passed := pass("foo")

			Convey("The action should pass", func() {
				So(passed, ShouldEqual, true)
			})
		})

		Convey("When I perform the same action three times", func() {
			flushall()
			var results [3]bool
			for i := 0; i < 3; i++ {
				results[i] = pass("foo")
			}

			Convey("All three actions should pass", func() {
				So(results, ShouldResemble, [3]bool{true, true, true})
			})

			Convey("When I peek", func() {
				count, err := stopper.Peek("foo")

				Convey("Count should be 3", func() {
					So(err, ShouldEqual, nil)
					So(count, ShouldEqual, 3)
				})
			})

			Convey("The fourth action should fail", func() {
				So(pass("foo"), ShouldEqual, false)

				Convey("And pass again after the interval", func() {
					clock.AddTime(stopper.Interval)
					So(pass("foo"), ShouldEqual, true)
				})
			})
		})

		Convey("When my actions are blocked", func() {
			flushall()
			var results [4]bool
			for i := 0; i < 4; i++ {
				results[i] = pass("foo")
			}
			So(results, ShouldResemble, [4]bool{true, true, true, false})

			Convey("Other actions should still pass", func() {
				var results [3]bool
				for i := 0; i < 3; i++ {
					results[i] = pass("bar")
				}
				So(results, ShouldResemble, [3]bool{true, true, true})
			})
		})
	})

	Convey("Given a stopper without an explicit clock", t, func() {
		stopper := Stopper{
			Namespace: "realstopperwithclock",
			Interval:  5 * time.Second,
			Limit:     int64(3),
			ConnPool:  &connPool,
		}

		Convey("It still works", func() {
			flushall()
			var results [4]bool
			for i := 0; i < 4; i++ {
				passed, err := stopper.Pass("foo")
				if err != nil {
					t.Fatal(err)
				}
				results[i] = passed
			}
			So(results, ShouldResemble, [4]bool{true, true, true, false})
		})
	})

}

func runRedisServer() *exec.Cmd {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	redisServer := exec.Command("redis-server", "--port", fmt.Sprintf("%d", redisServerPort))
	redisServer.Stdout = &stdout
	redisServer.Stderr = &stderr

	err := redisServer.Start()
	if err != nil {
		fmt.Println(err)
		return nil
	}
	go func() {
		err := redisServer.Wait()
		if err != nil {
			fmt.Printf("STDOUT: %s\n\nSTDERR: %s\n", stdout.String(), stderr.String())
		}
	}()
	attempt := 0
	for {
		time.Sleep(100 * time.Millisecond)
		attempt++
		if attempt > 100 {
			fmt.Println("redis-server failed to come up after 10 seconds")
			return nil
		}
		conn, err := redis.Dial("tcp", fmt.Sprintf("localhost:%d", redisServerPort))
		if err != nil {
			continue
		}
		_, err = conn.Do("PING")
		if err == nil {
			break
		}
	}
	return redisServer
}
