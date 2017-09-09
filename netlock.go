// Package netlock provides simple mutex-like primitive that uses Redis server
// to coordinate.
//
// It implements the algorithm described at
// https://redis.io/commands/set#patterns
//
// Caller tries to set unique key with given TTL (upper estimate of time to hold
// the mutex), if it succeeds, it can proceed with its work using context to
// check whether TTL expired and cancel function to both cancel the context and
// release the lock.
package netlock

import (
	"bufio"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/artyom/resp"
)

// Acquire attempts to acquire shared lock with given name on existing
// connection conn to Redist host. On success, the key name is set to expire
// after ttl or when context is canceled (if network connection is still alive).
// Returned context is set to cancel after ttl. CancelFunc should be used to
// release lock before it expires.
//
// On success conn is bound to CancelFunc so it shouldn't be reused until
// CancelFunc is called otherwise it would break Redis session state. Normally
// conn should be created by caller specifically to be used by Acquire and
// closed once CancelFunc is called.
func Acquire(ctx context.Context, conn io.ReadWriter, name string, ttl time.Duration) (context.Context, context.CancelFunc, error) {
	if ttl <= 0 {
		return nil, nil, errors.New("non-positive TTL")
	}
	ctx, cancel := context.WithTimeout(ctx, ttl)
	rd := bufio.NewReader(conn)
	uid := genUID()
	// https://redis.io/commands/set
	// SET key value [EX seconds] [PX milliseconds] [NX|XX]
	req := resp.Array{"SET", name, uid, "PX", strconv.Itoa(int(ttl / time.Millisecond)), "NX"}
	if err := resp.Encode(conn, req); err != nil {
		cancel()
		return nil, nil, err
	}
	res, err := resp.Decode(rd)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	if res == nil {
		cancel()
		return nil, nil, ErrLocked
	}
	if s, ok := res.(string); !ok || s != "OK" {
		cancel()
		return nil, nil, fmt.Errorf("bad response value: %#v", res)
	}
	var once sync.Once
	cancelFunc := func() {
		cancel()
		once.Do(func() {
			req := resp.Array{"EVAL", scriptBody, "1", name, uid}
			if err := resp.Encode(conn, req); err != nil {
				return
			}
			// read and ignore reply so connection is left in
			// consistent state
			_, _ = resp.Decode(rd)
		})
	}
	go func() { <-ctx.Done(); cancelFunc() }()
	return ctx, cancelFunc, nil
}

func genUID() string {
	b := make([]byte, 10)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return fmt.Sprintf("%x", b)
}

// scriptBody is listed here: https://redis.io/commands/set
const scriptBody = `if redis.call('get',KEYS[1]) == ARGV[1]
then
    return redis.call('del',KEYS[1])
else
    return 0
end
`

// ErrLocked is the error returned by Acquire if key already exists
var ErrLocked = errors.New("lock is already held")
