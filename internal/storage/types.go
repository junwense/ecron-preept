package storage

import (
	"context"
	"errors"
	"github.com/ecodeclub/ecron/internal/task"
	"time"
)

//go:generate mockgen -source=./types.go -package=daomocks -destination=./mocks/dao.mock.go
var (
	ErrFailedToPreempt = errors.New("抢占失败")
	ErrTaskNotHold     = errors.New("未持有任务")
)

type TaskRepository interface {
	//TryPreempt 抢占接口，会返回一批task给f方法
	TryPreempt(ctx context.Context, f func(ctx context.Context, ts []task.Task) (task.Task, error)) (task.Task, error)
	// PreemptTask 获取一个任务
	PreemptTask(ctx context.Context, tid int64, oldOwner string, newOwner string) error
	// ReleaseTask 释放任务
	ReleaseTask(ctx context.Context, tid int64, owner string) error
	// RefreshTask 续约
	RefreshTask(ctx context.Context, tid int64, owner string) error
}

type TaskCfgRepository interface {
	// Add 添加任务
	Add(ctx context.Context, t task.Task) error
	// Stop 停止任务
	Stop(ctx context.Context, id int64) error
	// UpdateNextTime 更新下次执行时间
	UpdateNextTime(ctx context.Context, id int64, next time.Time) error
}

// ExecutionDAO 任务执行情况
type ExecutionDAO interface {
	// Upsert 记录任务执行状态和进度
	Upsert(ctx context.Context, id int64, status task.ExecStatus, progress uint8) (int64, error)
}
