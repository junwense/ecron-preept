package preempt

import (
	"context"
	"errors"
	"github.com/ecodeclub/ecron/internal/task"
)

var (
	ErrPreemptHasRelease = errors.New("抢占任务已经被释放，退出自动续约")
)

//go:generate mockgen -source=./types.go -package=preemptmocks -destination=./mocks/preempt.mock.go
type Preempter interface {
	Preempt(ctx context.Context) (task.Task, error)

	// AutoRefresh 调用者调用ctx的cancel之后，才会关闭掉自动续约
	// 返回一个Status 的ch,有一定缓存，需要自行取走数据
	// 调用此方法的前提是已经 Preempt 抢占成功任务
	AutoRefresh(ctx context.Context, t task.Task) (s <-chan Status)

	// Refresh 保证幂等
	Refresh(ctx context.Context, t task.Task) error
	// Release 保证幂等
	Release(ctx context.Context, t task.Task) error
}

type Status interface {
	Err() error
}
