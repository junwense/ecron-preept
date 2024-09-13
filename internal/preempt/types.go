package preempt

import (
	"context"
	"errors"
	"github.com/ecodeclub/ecron/internal/task"
)

//go:generate mockgen -source=./types.go -package=preemptmocks -destination=./mocks/preempt.mock.go
var (
	ErrNoTaskToPreempt  = errors.New("没有任务可以抢占")
	ErrLeaserHasRelease = errors.New("抢占任务已经被释放,不能在进行操作")
)

// Preempter 成功会返回TaskLeaser
type Preempter interface {
	Preempt(ctx context.Context) (TaskLeaser, error)
}

// TaskLeaser 租约
type TaskLeaser interface {
	GetTask() task.Task

	// Refresh 保证幂等
	Refresh(ctx context.Context) error
	// Release 保证幂等，调用后，释放租约，不再能调用 Refresh/AutoRefresh
	Release(ctx context.Context) error

	// AutoRefresh
	// 返回一个Status 的ch,有一定缓存，需要自行取走数据
	// 如果ctx到期，会结束当前AutoRefresh
	AutoRefresh(ctx context.Context) (s <-chan Status, err error)
}

type Status interface {
	Err() error
}
