package preempt

import (
	"context"
	"errors"
	"github.com/ecodeclub/ecron/internal/storage"
	"github.com/ecodeclub/ecron/internal/task"
	"github.com/google/uuid"
	"log/slog"
	"math/rand"
	"time"
)

type DefaultPreempter struct {
	taskRepository storage.TaskRepository
	logger         *slog.Logger
	// with options
	refreshTimeout  time.Duration
	refreshInterval time.Duration
	buffSize        uint8
	maxRetryTimes   uint8
	retrySleepTime  time.Duration
	randIndex       func(num int) int
}

func NewDefaultPreempter(dao storage.TaskRepository) *DefaultPreempter {
	return &DefaultPreempter{
		taskRepository: dao,

		refreshTimeout:  2 * time.Second,
		refreshInterval: time.Second * 5,
		buffSize:        3,
		maxRetryTimes:   3,
		retrySleepTime:  time.Second,
		randIndex: func(num int) int {
			return rand.Intn(num)
		},
	}
}

func (d *DefaultPreempter) Preempt(ctx context.Context) (task.Task, error) {
	return d.taskRepository.TryPreempt(ctx, func(ctx context.Context, tasks []task.Task) (task.Task, error) {
		size := len(tasks)
		index := d.randIndex(size)
		var err error
		for i := index % size; i < size; i++ {
			t := tasks[i]
			value := uuid.New().String()
			err = d.taskRepository.PreemptTask(ctx, t.ID, t.Owner, value)
			if err == nil {
				t.Owner = value
				return t, nil
			}
		}
		return task.Task{}, err
	})
}

func (d *DefaultPreempter) AutoRefresh(ctx context.Context, t task.Task) (s <-chan Status) {
	sch := make(chan Status, d.buffSize)
	go func() {
		defer func() {
			err := d.Release(ctx, t)
			if err != nil {
				d.logger.Error("停止任务失败", slog.Int64("task_id", t.ID), slog.Any("error", err))
			}
		}()
		err := d.autoRefresh(ctx, t)
		select {
		case sch <- NewDefaultLeaseStatus(err):
		default:

		}
	}()

	return sch
}

// autoRefresh 控制 ctx.Done 和 ticker
func (d *DefaultPreempter) autoRefresh(ctx context.Context, t task.Task) error {
	ticker := time.NewTicker(d.refreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			err := d.refreshTask(ctx, ticker, t)
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// refreshTask 控制重试和真的执行刷新
func (d *DefaultPreempter) refreshTask(ctx context.Context, ticker *time.Ticker, t task.Task) error {
	var i = 0
	var err error
	for {
		if i >= int(d.maxRetryTimes) {
			return err
		}
		err = d.Refresh(ctx, t)
		switch {
		case err == nil:
			ticker.Reset(d.refreshInterval)
			return nil
		case errors.Is(err, context.DeadlineExceeded):
			i++
			time.Sleep(d.retrySleepTime)
		default:
			return err
		}
	}
}

func (d *DefaultPreempter) Refresh(ctx context.Context, t task.Task) error {
	ctx, cancel := context.WithTimeout(ctx, d.refreshTimeout)
	defer cancel()
	err := d.taskRepository.RefreshTask(ctx, t.ID, t.Owner)
	return err
}

func (d *DefaultPreempter) Release(ctx context.Context, t task.Task) error {
	// 解除续约的时候可以不用原始上下文,因为超时了也需要解除,这里保持幂等就可以
	ctx, cancel := context.WithTimeout(context.Background(), d.refreshTimeout)
	defer cancel()
	return d.taskRepository.ReleaseTask(ctx, t.ID, t.Owner)
}
