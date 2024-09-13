package preempt

import (
	"context"
	"errors"
	"github.com/ecodeclub/ecron/internal/storage"
	"github.com/ecodeclub/ecron/internal/task"
	"github.com/google/uuid"
	"math/rand"
	"sync"
	"sync/atomic"
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
}

func NewDefaultPreempter(dao storage.TaskRepository) *DefaultPreempter {
	return &DefaultPreempter{
		taskRepository:  dao,
		refreshTimeout:  2 * time.Second,
		refreshInterval: time.Second * 5,
		buffSize:        10,
		maxRetryTimes:   3,
		retrySleepTime:  time.Second,
		randIndex: func(num int) int {
			return rand.Intn(num)
		},
	}
}

func (p *DefaultPreempter) Preempt(ctx context.Context) (TaskLeaser, error) {
	t, err := p.taskRepository.TryPreempt(ctx, func(ctx context.Context, tasks []task.Task) (task.Task, error) {
		size := len(tasks)
		index := p.randIndex(size)
		var err error
		for i := index % size; i < size; i++ {
			t := tasks[i]
			value := uuid.New().String()
			err = p.taskRepository.PreemptTask(ctx, t.ID, t.Owner, value)
			switch {
			case err == nil:
				t.Owner = value
				return t, nil
			case errors.Is(err, storage.ErrFailedToPreempt):
				continue
			default:
				return task.Task{}, err
			}
		}
		return task.Task{}, ErrNoTaskToPreempt
	})
	if err != nil {
		return nil, err
	}
	var b atomic.Bool
	b.Store(false)
	return &DefaultTaskLeaser{
		t:               t,
		taskRepository:  p.taskRepository,
		refreshTimeout:  p.refreshTimeout,
		refreshInterval: p.refreshInterval,
		buffSize:        p.buffSize,
		maxRetryTimes:   p.maxRetryTimes,
		retrySleepTime:  p.retrySleepTime,
		done:            make(chan struct{}),
		ones:            sync.Once{},
		hasDone:         b,
	}, err
}

type DefaultTaskLeaser struct {
	t               task.Task
	taskRepository  storage.TaskRepository
	refreshTimeout  time.Duration
	refreshInterval time.Duration
	buffSize        uint8
	maxRetryTimes   uint8
	retrySleepTime  time.Duration
	ones            sync.Once
	done            chan struct{}
	hasDone         atomic.Bool
}

func (d *DefaultTaskLeaser) Refresh(ctx context.Context) error {
	if d.hasDone.Load() {
		return ErrLeaserHasRelease
	}
	return d.taskRepository.RefreshTask(ctx, d.t.ID, d.t.Owner)
}

func (d *DefaultTaskLeaser) Release(ctx context.Context) error {
	d.ones.Do(func() {
		d.hasDone.Store(true)
		close(d.done)
	})
	if d.hasDone.Load() {
		return ErrLeaserHasRelease
	}
	return d.taskRepository.ReleaseTask(ctx, d.t.ID, d.t.Owner)
}

func (d *DefaultTaskLeaser) GetTask() task.Task {
	return d.t
}

func (d *DefaultTaskLeaser) AutoRefresh(ctx context.Context) (s <-chan Status, err error) {
	if d.hasDone.Load() {
		return nil, ErrLeaserHasRelease
	}
	sch := make(chan Status, d.buffSize)
	go func() {
		defer close(sch)
		ticker := time.NewTicker(d.refreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				send2Ch(sch, NewDefaultStatus(d.refreshTask(ctx)))
			case <-d.done:
				send2Ch(sch, NewDefaultStatus(ErrLeaserHasRelease))
				return
			case <-ctx.Done():
				send2Ch(sch, NewDefaultStatus(ctx.Err()))
				return
			}
		}
	}()

	return sch, nil
}

// refreshTask 控制重试和真的执行刷新,续约间隔 > 当次续约（含重试）的所有时间
func (d *DefaultTaskLeaser) refreshTask(ctx context.Context) error {
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
		err = d.Refresh(ctx1)
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

func send2Ch(ch chan<- Status, st Status) {
	select {
	case ch <- st:
	default:

	}
}
