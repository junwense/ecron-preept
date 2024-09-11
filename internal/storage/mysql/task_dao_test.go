package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/ecodeclub/ecron/internal/errs"
	"github.com/ecodeclub/ecron/internal/storage"
	"github.com/ecodeclub/ecron/internal/task"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"testing"
	"time"
)

func TestGormTaskRepository_TryPreempt(t *testing.T) {
	testCases := []struct {
		name            string
		batchSize       int
		refreshInterval time.Duration
		sqlMock         func(t *testing.T) *sql.DB
		selectMock      func(ctx context.Context, ts []task.Task) (task.Task, error)
		wantTask        task.Task
		wantErr         error
	}{
		{
			name:            "查询数据库错误",
			batchSize:       10,
			refreshInterval: 10 * time.Second,
			sqlMock: func(t *testing.T) *sql.DB {
				mockDB, mock, err := sqlmock.New()
				require.NoError(t, err)
				mock.ExpectQuery("^SELECT \\* FROM `task_info`").
					WillReturnError(errors.New("select error"))
				return mockDB
			},
			wantTask: task.Task{},
			wantErr:  errors.New("select error"),
		},
		{
			name:            "当前没有可以执行的任务",
			batchSize:       10,
			refreshInterval: 10 * time.Second,
			sqlMock: func(t *testing.T) *sql.DB {
				mockDB, mock, err := sqlmock.New()
				require.NoError(t, err)
				// 返回0行
				mock.ExpectQuery("^SELECT \\* FROM `task_info`").WillReturnRows(sqlmock.NewRows(nil))
				return mockDB
			},
			wantTask: task.Task{},
			wantErr:  errs.ErrNoExecutableTask,
		},
		{
			name:            "获取任务并抢占成功",
			batchSize:       10,
			refreshInterval: 10 * time.Second,
			sqlMock: func(t *testing.T) *sql.DB {
				mockDB, mock, err := sqlmock.New()
				require.NoError(t, err)
				// 查询结果只返回一条任务
				values := [][]driver.Value{
					{
						1,
						"test1",
						task.TypeLocal,
						"*/5 * * * * ?",
						"local",
						5,
						"owner",
						TaskStatusWaiting,
						"",
						time.Now().UnixMilli(),
						time.Now().UnixMilli(),
						time.Now().UnixMilli(),
					},
				}
				rows := sqlmock.NewRows([]string{
					"id", "name", "type",
					"cron", "executor", "Version", "Owner", "Status", "cfg",
					"next_exec_time", "ctime", "utime",
				}).AddRows(values...)
				mock.ExpectQuery("^SELECT \\* FROM `task_info`").WillReturnRows(rows)
				mock.ExpectExec("UPDATE `task_info`").WillReturnResult(sqlmock.NewResult(1, 1))
				return mockDB
			},
			wantTask: task.Task{
				ID:       1,
				Name:     "test1",
				Type:     task.TypeLocal,
				Executor: "local",
				Cfg:      "",
				Owner:    "owner",
				CronExp:  "*/5 * * * * ?",
			},
			selectMock: func(ctx context.Context, ts []task.Task) (task.Task, error) {
				for _, t2 := range ts {
					t := t2
					if t.ID == 1 {
						return t, nil
					}
				}
				return task.Task{}, storage.ErrFailedToPreempt
			},
			wantErr: nil,
		},
		{
			name:            "获取任务并抢占失败",
			batchSize:       10,
			refreshInterval: 10 * time.Second,
			sqlMock: func(t *testing.T) *sql.DB {
				mockDB, mock, err := sqlmock.New()
				require.NoError(t, err)
				// 查询结果只返回一条任务
				values := [][]driver.Value{
					{
						0,
						"test1",
						task.TypeLocal,
						"*/5 * * * * ?",
						"local",
						5, "owner", TaskStatusWaiting,
						"",
						time.Now().UnixMilli(),
						time.Now().UnixMilli(),
						time.Now().UnixMilli(),
					},
				}
				rows := sqlmock.NewRows([]string{
					"id", "name", "type",
					"cron", "executor", "Version", "Owner", "Status", "cfg",
					"next_exec_time", "ctime", "utime",
				}).AddRows(values...)
				mock.ExpectQuery("^SELECT \\* FROM `task_info`").WillReturnRows(rows)
				mock.ExpectExec("UPDATE `task_info`").WillReturnResult(sqlmock.NewResult(1, 1))
				return mockDB
			},
			selectMock: func(ctx context.Context, ts []task.Task) (task.Task, error) {
				for _, t2 := range ts {
					t := t2
					if t.ID == 1 {
						return t, nil
					}
				}
				return task.Task{}, storage.ErrFailedToPreempt
			},
			wantErr: storage.ErrFailedToPreempt,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sqlDB := tc.sqlMock(t)
			db, err := gorm.Open(mysql.New(mysql.Config{
				Conn:                      sqlDB,
				SkipInitializeWithVersion: true,
			}), &gorm.Config{
				DisableAutomaticPing:   true,
				SkipDefaultTransaction: true,
			})
			require.NoError(t, err)

			dao := NewGormTaskRepository(db, tc.batchSize, tc.refreshInterval)

			res, err := dao.TryPreempt(context.Background(), tc.selectMock)
			if err != nil {
				assert.Equal(t, tc.wantErr, err)
				return
			}
			assert.Equal(t, tc.wantTask.ID, res.ID)
			assert.Equal(t, tc.wantTask.Name, res.Name)
			assert.Equal(t, tc.wantTask.Type, res.Type)
			assert.Equal(t, tc.wantTask.Executor, res.Executor)
			assert.Equal(t, tc.wantTask.CronExp, res.CronExp)
			assert.Equal(t, tc.wantTask.Owner, res.Owner)
			assert.True(t, res.Ctime.UnixMilli() > 0)
			assert.True(t, res.Utime.UnixMilli() > 0)
		})
	}
}
func TestGormTaskRepository_PreemptTask(t *testing.T) {

	zero := task.Task{
		ID:    1,
		Owner: "tom",
	}
	testCases := []struct {
		name            string
		batchSize       int
		refreshInterval time.Duration
		sqlMock         func(t *testing.T) *sql.DB
		tid             int64
		old             string
		new             string
		wantErr         error
	}{
		{
			name:            "抢占成功",
			batchSize:       10,
			refreshInterval: 10 * time.Second,
			sqlMock: func(t *testing.T) *sql.DB {
				mockDB, mock, err := sqlmock.New()
				require.NoError(t, err)
				//mock.ExpectExec("UPDATE `task_info`").WithArgs(zero.ID, zero.Owner).WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectExec("UPDATE `task_info`").
					WithArgs("jack", sqlmock.AnyArg(), sqlmock.AnyArg(), zero.ID, zero.Owner).WillReturnResult(sqlmock.NewResult(1, 1))
				return mockDB
			},
			tid:     zero.ID,
			old:     zero.Owner,
			new:     "jack",
			wantErr: nil,
		},
		{
			name:            "抢占失败",
			batchSize:       10,
			refreshInterval: 10 * time.Second,
			sqlMock: func(t *testing.T) *sql.DB {
				mockDB, mock, err := sqlmock.New()
				require.NoError(t, err)
				mock.ExpectExec("UPDATE `task_info`").WithArgs("jack", sqlmock.AnyArg(), sqlmock.AnyArg(), zero.ID, zero.Owner).WillReturnResult(sqlmock.NewResult(0, 0))
				return mockDB
			},
			tid:     zero.ID,
			old:     "jack",
			new:     "tom",
			wantErr: storage.ErrFailedToPreempt,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sqlDB := tc.sqlMock(t)
			db, err := gorm.Open(mysql.New(mysql.Config{
				Conn:                      sqlDB,
				SkipInitializeWithVersion: true,
			}), &gorm.Config{
				DisableAutomaticPing:   true,
				SkipDefaultTransaction: true,
			})
			require.NoError(t, err)

			dao := NewGormTaskRepository(db, tc.batchSize, tc.refreshInterval)

			err = dao.PreemptTask(context.Background(), tc.tid, tc.old, tc.new)
			if err != nil {
				assert.Equal(t, tc.wantErr, err)
				return
			}
		})
	}
}

func TestGormTaskRepository_RefreshTask(t *testing.T) {

	zero := task.Task{
		ID:    1,
		Owner: "tom",
	}
	testCases := []struct {
		name            string
		batchSize       int
		refreshInterval time.Duration
		sqlMock         func(t *testing.T) *sql.DB
		tid             int64
		owner           string
		wantErr         error
		status          int8
	}{
		{
			name:            "续约成功",
			batchSize:       10,
			refreshInterval: 10 * time.Second,
			sqlMock: func(t *testing.T) *sql.DB {
				mockDB, mock, err := sqlmock.New()
				require.NoError(t, err)
				//mock.ExpectExec("UPDATE `task_info`").WithArgs(zero.ID, zero.Owner).WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectExec("UPDATE `task_info`").
					WithArgs(sqlmock.AnyArg(), zero.ID, zero.Owner, TaskStatusRunning).WillReturnResult(sqlmock.NewResult(1, 1))
				return mockDB
			},
			tid:     zero.ID,
			owner:   zero.Owner,
			status:  TaskStatusRunning,
			wantErr: nil,
		},
		{
			name:            "续约失败",
			batchSize:       10,
			refreshInterval: 10 * time.Second,
			sqlMock: func(t *testing.T) *sql.DB {
				mockDB, mock, err := sqlmock.New()
				require.NoError(t, err)
				mock.ExpectExec("UPDATE `task_info`").WithArgs(sqlmock.AnyArg(), zero.ID, zero.Owner, TaskStatusRunning).WillReturnResult(sqlmock.NewResult(0, 0))
				return mockDB
			},
			tid:     zero.ID,
			owner:   "jack",
			status:  TaskStatusRunning,
			wantErr: storage.ErrTaskNotHold,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sqlDB := tc.sqlMock(t)
			db, err := gorm.Open(mysql.New(mysql.Config{
				Conn:                      sqlDB,
				SkipInitializeWithVersion: true,
			}), &gorm.Config{
				DisableAutomaticPing:   true,
				SkipDefaultTransaction: true,
			})
			require.NoError(t, err)
			dao := NewGormTaskRepository(db, tc.batchSize, tc.refreshInterval)
			err = dao.RefreshTask(context.Background(), tc.tid, tc.owner)
			if err != nil {
				assert.Equal(t, tc.wantErr, err)
				return
			}
		})
	}
}

func TestGormTaskRepository_ReleaseTask(t *testing.T) {

	zero := task.Task{
		ID:    1,
		Owner: "tom",
	}
	testCases := []struct {
		name            string
		batchSize       int
		refreshInterval time.Duration
		sqlMock         func(t *testing.T) *sql.DB
		tid             int64
		owner           string
		wantErr         error
	}{
		{
			name:            "解除成功",
			batchSize:       10,
			refreshInterval: 10 * time.Second,
			sqlMock: func(t *testing.T) *sql.DB {
				mockDB, mock, err := sqlmock.New()
				require.NoError(t, err)
				//mock.ExpectExec("UPDATE `task_info`").WithArgs(zero.ID, zero.Owner).WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectExec("UPDATE `task_info`").
					WithArgs(TaskStatusWaiting, sqlmock.AnyArg(), zero.ID, zero.Owner).WillReturnResult(sqlmock.NewResult(1, 1))
				return mockDB
			},
			tid:     zero.ID,
			owner:   zero.Owner,
			wantErr: nil,
		},
		{
			name:            "解除失败",
			batchSize:       10,
			refreshInterval: 10 * time.Second,
			sqlMock: func(t *testing.T) *sql.DB {
				mockDB, mock, err := sqlmock.New()
				require.NoError(t, err)
				mock.ExpectExec("UPDATE `task_info`").WithArgs(TaskStatusWaiting, sqlmock.AnyArg(), zero.ID, zero.Owner).WillReturnResult(sqlmock.NewResult(0, 0))
				return mockDB
			},
			tid:     zero.ID,
			owner:   "jack",
			wantErr: storage.ErrTaskNotHold,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sqlDB := tc.sqlMock(t)
			db, err := gorm.Open(mysql.New(mysql.Config{
				Conn:                      sqlDB,
				SkipInitializeWithVersion: true,
			}), &gorm.Config{
				DisableAutomaticPing:   true,
				SkipDefaultTransaction: true,
			})
			require.NoError(t, err)
			dao := NewGormTaskRepository(db, tc.batchSize, tc.refreshInterval)
			err = dao.ReleaseTask(context.Background(), tc.tid, tc.owner)
			if err != nil {
				assert.Equal(t, tc.wantErr, err)
				return
			}
		})
	}
}
