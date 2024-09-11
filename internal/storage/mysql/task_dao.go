package mysql

import (
	"context"
	"github.com/ecodeclub/ecron/internal/errs"
	"github.com/ecodeclub/ecron/internal/storage"
	"github.com/ecodeclub/ecron/internal/task"
	"gorm.io/gorm"
	"time"
)

type GormTaskRepository struct {
	db              *gorm.DB
	batchSize       int
	refreshInterval time.Duration
}

func NewGormTaskRepository(db *gorm.DB, batchSize int, refreshInterval time.Duration) *GormTaskRepository {
	return &GormTaskRepository{
		db:              db,
		batchSize:       batchSize,
		refreshInterval: refreshInterval,
	}
}

// ReleaseTask 如果释放时释放了别人的抢到的任务怎么办，这里通过Owner控制
func (g *GormTaskRepository) ReleaseTask(ctx context.Context, tid int64, owner string) error {
	res := g.db.WithContext(ctx).Model(&TaskInfo{}).
		Where("id = ? AND owner = ?", tid, owner).
		Updates(map[string]interface{}{
			"status": TaskStatusWaiting,
			"utime":  time.Now().UnixMilli(),
		})

	if res.RowsAffected > 0 {
		return nil
	}
	return storage.ErrTaskNotHold
}

func (g *GormTaskRepository) RefreshTask(ctx context.Context, tid int64, owner string) error {
	res := g.db.WithContext(ctx).Model(&TaskInfo{}).
		Where("id = ? AND owner = ? AND status = ?", tid, owner, TaskStatusRunning).
		Updates(map[string]any{
			"utime": time.Now().UnixMilli(),
		})
	if res.RowsAffected > 0 {
		return nil
	}
	return storage.ErrTaskNotHold
}

func (g *GormTaskRepository) TryPreempt(ctx context.Context, f func(ctx context.Context, ts []task.Task) (task.Task, error)) (task.Task, error) {
	now := time.Now()
	var zero = task.Task{}
	// 续约的最晚时间
	t := now.UnixMilli() - g.refreshInterval.Milliseconds()
	var tasks []TaskInfo
	// 一次取一批
	err := g.db.WithContext(ctx).Model(&TaskInfo{}).
		Where("status = ? AND next_exec_time <= ?", TaskStatusWaiting, now.UnixMilli()).
		Or("status = ? AND utime < ?", TaskStatusRunning, t).
		Find(&tasks).Limit(g.batchSize).Error
	if err != nil {
		return zero, err
	}
	if len(tasks) < 1 {
		return zero, errs.ErrNoExecutableTask
	}
	ts := make([]task.Task, len(tasks))
	for i := 0; i < len(tasks); i++ {
		ts[i] = toTask(tasks[i])
	}
	return f(ctx, ts)
}

func (g *GormTaskRepository) PreemptTask(ctx context.Context, tid int64, oldOwner string, newOwner string) error {
	res := g.db.WithContext(ctx).Model(&TaskInfo{}).
		Where("id = ? AND owner = ?", tid, oldOwner).
		Updates(map[string]interface{}{
			"status": TaskStatusRunning,
			"utime":  time.Now().UnixMilli(),
			"owner":  newOwner,
		})
	if res.RowsAffected > 0 {
		return nil
	}
	return storage.ErrFailedToPreempt
}
