package flowstopper

import (
	"fmt"
	"github.com/WatchBeam/clock"
	"github.com/garyburd/redigo/redis"
	"time"
)

// Stopper is an instance of a rate limiter.
type Stopper struct {
	// The pool to take redis connections from.
	ConnPool *redis.Pool

	// The key prefix to use for the name in redis.
	Namespace string

	// The duration for which actions are tracked.
	Interval time.Duration

	// The maximum amount of actions allowed during the Interval.
	Limit int64

	c clock.Clock
}

func (s *Stopper) Pass(item string) (bool, error) {
	var now time.Time
	if s.c == nil {
		now = time.Now().UTC()
	} else {
		now = s.c.Now().UTC()
	}
	nanonow := now.UnixNano()
	key := fmt.Sprintf("%s:%s", s.Namespace, item)

	c := s.ConnPool.Get()
	defer c.Close()

	c.Send("MULTI")
	c.Send("ZREMRANGEBYSCORE", key, "-inf", now.Add(s.Interval*-1).UnixNano())
	c.Send("ZADD", key, nanonow, nanonow)
	c.Send("ZCARD", key)

	values, err := redis.Values(c.Do("EXEC"))
	if err != nil {
		return false, err
	}

	var remcount, addcount, setsize int64
	_, err = redis.Scan(values, &remcount, &addcount, &setsize)
	if err != nil {
		return false, err
	}

	if setsize > s.Limit {
		return false, nil
	} else {
		return true, nil
	}
}