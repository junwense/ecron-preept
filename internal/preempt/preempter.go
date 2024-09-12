package preempt

import (
	"context"
	"errors"
	"github.com/ecodeclub/ecron/internal/storage"
	"github.com/ecodeclub/ecron/internal/task"
	"github.com/google/uuid"
	"math/rand"
	"sync"
	"time"
)

type DefaultPreempter struct {
	taskRepository storage.TaskRepository
	// with options
	refreshTimeout  time.Duration
	refreshInterval time.Duration
	buffSize        uint8
	maxRetryTimes   uint8
	retrySleepTime  time.Duration
	randIndex       func(num int) int
	ones            sync.Once
	done            chan struct{}
}

func NewDefaultPreempter(dao storage.TaskRepository) *DefaultPreempter {
	return &DefaultPreempter{
		taskRepository: dao,

		refreshTimeout:  2 * time.Second,
		refreshInterval: time.Second * 5,
		buffSize:        10,
		maxRetryTimes:   3,
		retrySleepTime:  time.Second,
		randIndex: func(num int) int {
			return rand.Intn(num)
		},
		done: make(chan struct{}),
		ones: sync.Once{},
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
		defer close(sch)
		ticker := time.NewTicker(d.refreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				send2Ch(sch, NewDefaultStatus(d.refreshTask(ctx, t)))
			case <-d.done:
				send2Ch(sch, NewDefaultStatus(ErrPreemptHasRelease))
				return
			case <-ctx.Done():
				send2Ch(sch, NewDefaultStatus(ctx.Err()))
				return
			}
		}
	}()

	return sch
}

func send2Ch(ch chan<- Status, st Status) {
	select {
	case ch <- st:
	default:

	}
}

// refreshTask 控制重试和真的执行刷新,续约间隔 > 当次续约（含重试）的所有时间
func (d *DefaultPreempter) refreshTask(ctx context.Context, t task.Task) error {
	var i = 0
	var err error
	for {
		if ctx.Err() != nil {
			return err
		}

		if i >= int(d.maxRetryTimes) {
			// 这个分支是超时一定次数失败了
			return err
		}
		ctx1, cancel := context.WithTimeout(ctx, d.refreshTimeout)
		err = d.Refresh(ctx1, t)
		cancel()
		switch {
		case err == nil:
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
	return d.taskRepository.RefreshTask(ctx, t.ID, t.Owner)
}

func (d *DefaultPreempter) Release(ctx context.Context, t task.Task) error {
	d.ones.Do(func() {
		close(d.done)
	})
	return d.taskRepository.ReleaseTask(ctx, t.ID, t.Owner)
}
