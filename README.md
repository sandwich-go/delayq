# 延迟队列

- 支持内存级别的延迟队列
- 支持分布式延迟队列
- 队列状态监控

```go
import (
    "github.com/sandwich-go/delayq"
)

func main(){
    dq := delayq.New()
    err := dq.Start("test", func(item *delayq.Item) error {
        fmt.Println(item)
    })
    if err != nil {
        return
    }
    item := &delayq.Item{DelaySecond: int64(1), Value: []byte("best")}
    err = dq.Push(item)
    if err != nil {
        return
    }
}
```

## 内存式延迟队列
```go
import (
    "github.com/sandwich-go/delayq"
)

func main(){
    dq := delayq.New()
    err := dq.Start("test", func(item *delayq.Item) error {
        fmt.Println(item)
    })
    if err != nil {
        return
    }
}
```

## 分布式延迟队列
```go
import (
    "context"
    "github.com/sandwich-go/delayq"
    "github.com/sandwich-go/redisson"
)

type scriptBuilder struct{ c redisson.Cmdable }
type script struct{ s redisson.Scripter }

func (s scriptBuilder) Build(src string) delayq.RedisScript {
    return script{s: s.c.CreateScript(src)}
}

func (s script) EvalSha(ctx context.Context, keys []string, args ...interface{}) ([]interface{}, error) {
    return s.s.EvalSha(ctx, keys, args...).Slice()
}

func (s script) Eval(ctx context.Context, keys []string, args ...interface{}) ([]interface{}, error) {
    return s.s.Eval(ctx, keys, args...).Slice()
}

func main(){
    var c redisson.Cmdable
    // ... 连接 redis ...

    // 如果 RedisScriptBuilder 不为 nil，则为分布式延迟队列
    dq := delayq.New(delayq.WithRedisScriptBuilder(&scriptBuilder{c}))
    err := dq.Start("test", func(item *delayq.Item) error {
        fmt.Println(item)
    })
    if err != nil {
        return
    }	
}
```

### 参数
```go
// Options should use newConfig to initialize it
type Options struct {
    // annotation@Name(comment="名称")
    Name string
    // annotation@Prefix(comment="前缀")
    Prefix string
    // annotation@RedisScriptBuilder(comment="redis 脚本工厂")
    RedisScriptBuilder RedisScriptBuilder
    // annotation@RetryTimes(comment="重试次数")
    RetryTimes int
    // annotation@OnDeadLetter(comment="当有死信")
    OnDeadLetter func(item *Item)
    // annotation@MonitorCounter(comment="监控统计函数")
    MonitorCounter func(metric string, value int64, labels prometheus.Labels)
}
```