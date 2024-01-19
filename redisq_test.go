package delayq

/**
import (
	"context"
	"fmt"
	"github.com/sandwich-go/redisson"
	"testing"
	"time"
)

type redisScriptBuilder struct {
	s redisson.Cmdable
}

func newRedisScriptBuilder() RedisScriptBuilder {
	return redisScriptBuilder{redisson.MustNewClient(redisson.NewConf())}
}

type redisScript struct {
	s redisson.Scripter
}

func (r redisScriptBuilder) Build(src string) RedisScript {
	return redisScript{r.s.CreateScript(src)}
}

func (r redisScript) EvalSha(ctx context.Context, keys []string, args ...interface{}) ([]interface{}, error) {
	return r.s.EvalSha(ctx, keys, args...).Slice()
}
func (r redisScript) Eval(ctx context.Context, keys []string, args ...interface{}) ([]interface{}, error) {
	return r.s.Eval(ctx, keys, args...).Slice()
}

func TestRedisScript(t *testing.T) {
	tp := NewRedisTopicQueue(context.Background(), "test", nil, WithRedisScriptBuilder(newRedisScriptBuilder()), WithRetryTimes(2))
	item := &Item{DelaySecond: 1, Value: []byte("best")}
	var length = func(should int64) {
		if l := tp.Length(); l != should {
			t.Fatalf("length should eq %d, now %d", should, l)
		}
	}
	var poll = func() {
		err := tp.(*redisQueue).poll()
		if err != nil {
			t.Fatal("poll item error", err)
		}
	}
	var push = func() {
		err := tp.Push(item)
		if err != nil {
			t.Fatal("push item error", err)
		}
	}
	var handler = func(err error) {
		tp.(*redisQueue).handle = func(item *Item) error {
			return err
		}
	}
	push()
	err := tp.(*redisQueue).onSuccess(item)
	if err != nil {
		t.Fatal("ack item error", err)
	}
	push()
	length(1)
	time.Sleep(time.Duration(item.GetDelaySecond()*2) * time.Second)
	poll()
	length(0)
	handler(fmt.Errorf("errors"))
	push()
	poll()
	time.Sleep(10 * time.Millisecond)
	poll()
	time.Sleep(10 * time.Millisecond)
	poll()
	length(0)
	push()
	_, err = tp.(*redisQueue).move(tp.(*redisQueue).delaySetKey, tp.(*redisQueue).doingSetKey, 1)
	if err != nil {
		t.Fatal("poll item error", err)
	}
	length(0)
	time.Sleep(1 * time.Second)
	err = tp.(*redisQueue).reclaim()
	if err != nil {
		t.Fatal("reclaim item error", err)
	}
	length(1)
	handler(nil)
	time.Sleep(10 * time.Millisecond)
	poll()
	time.Sleep(10 * time.Millisecond)
	length(0)
}
**/
