package preempt

import (
	"errors"
	"github.com/ecodeclub/ecron/internal/storage"
	daomocks "github.com/ecodeclub/ecron/internal/storage/mocks"
	"github.com/ecodeclub/ecron/internal/task"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"golang.org/x/net/context"
	"testing"
	"time"
)

func TestPreemptScheduler_Preempt(t *testing.T) {
	testCases := []struct {
		name     string
		mock     func(ctrl *gomock.Controller) storage.TaskRepository
		wantErr  error
		ctxFn    func() context.Context
		wantTask TaskLeaser
	}{
		{
			name: "抢占成功",
			mock: func(ctrl *gomock.Controller) storage.TaskRepository {
				td := daomocks.NewMockTaskRepository(ctrl)

				td.EXPECT().TryPreempt(gomock.Any(), gomock.Any()).Return(task.Task{
					ID:      1,
					Version: 1,
				}, nil)

				return td
			},
			wantTask: &DefaultTaskLeaser{
				t: task.Task{
					ID:      1,
					Version: 1,
				},
			},
			ctxFn: func() context.Context {
				return context.Background()
			},
		},
		{
			name: "抢占失败，没有任务了",
			mock: func(ctrl *gomock.Controller) storage.TaskRepository {
				td := daomocks.NewMockTaskRepository(ctrl)

				td.EXPECT().TryPreempt(gomock.Any(), gomock.Any()).Return(task.Task{}, storage.ErrFailedToPreempt)

				return td
			},
			wantErr: storage.ErrFailedToPreempt,
			ctxFn: func() context.Context {
				return context.Background()
			},
		},
		{
			name: "抢占失败，超时",
			mock: func(ctrl *gomock.Controller) storage.TaskRepository {
				td := daomocks.NewMockTaskRepository(ctrl)

				td.EXPECT().TryPreempt(gomock.Any(), gomock.Any()).Return(task.Task{}, context.DeadlineExceeded)

				return td
			},
			wantErr: context.DeadlineExceeded,
			ctxFn: func() context.Context {

				ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond*1)
				defer cancel()
				return ctx
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			td := tc.mock(ctrl)

			preempter := NewDefaultPreempter(td)
			fn := tc.ctxFn()
			t1, err := preempter.Preempt(fn)
			assert.Equal(t, tc.wantErr, err)
			if err == nil {
				assert.Equal(t, tc.wantTask.GetTask(), t1.GetTask())
			}
		})
	}
}

func TestPreemptScheduler_TaskLeaser_AutoRefresh(t *testing.T) {
	testCases := []struct {
		name    string
		mock    func(ctrl *gomock.Controller) storage.TaskRepository
		wantErr error
		ctxFn   func() context.Context
	}{
		{
			name: "UpdateUtime error",
			mock: func(ctrl *gomock.Controller) storage.TaskRepository {
				td := daomocks.NewMockTaskRepository(ctrl)

				t := task.Task{
					ID:    1,
					Owner: "tom",
				}
				td.EXPECT().TryPreempt(gomock.Any(), gomock.Any()).Return(t, nil)
				//td.EXPECT().PreemptTask(gomock.Any(), t, "1-appid").Return(nil)
				td.EXPECT().RefreshTask(gomock.Any(), t.ID, t.Owner).AnyTimes().Return(errors.New("UpdateUtime error"))
				td.EXPECT().ReleaseTask(gomock.Any(), t.ID, t.Owner).Return(nil)

				return td
			},
			ctxFn: func() context.Context {
				return context.Background()
			},
			wantErr: errors.New("UpdateUtime error"),
		},
		{
			name: "context超时了",
			mock: func(ctrl *gomock.Controller) storage.TaskRepository {
				td := daomocks.NewMockTaskRepository(ctrl)

				t := task.Task{
					ID:    1,
					Owner: "tom",
				}
				td.EXPECT().TryPreempt(gomock.Any(), gomock.Any()).Return(t, nil)

				td.EXPECT().RefreshTask(gomock.Any(), t.ID, t.Owner).AnyTimes().Return(context.DeadlineExceeded)
				td.EXPECT().ReleaseTask(gomock.Any(), t.ID, t.Owner).Return(nil)

				return td
			},
			wantErr: context.DeadlineExceeded,
			ctxFn: func() context.Context {
				return context.Background()
			},
		},
		{
			name: "正常结束（context被取消）",
			mock: func(ctrl *gomock.Controller) storage.TaskRepository {
				td := daomocks.NewMockTaskRepository(ctrl)

				t := task.Task{
					ID:    1,
					Owner: "tom",
				}
				td.EXPECT().TryPreempt(gomock.Any(), gomock.Any()).Return(t, nil)

				td.EXPECT().RefreshTask(gomock.Any(), t.ID, t.Owner).AnyTimes().Return(nil)
				td.EXPECT().ReleaseTask(gomock.Any(), t.ID, t.Owner).Return(nil)

				return td
			},
			wantErr: context.Canceled,
			ctxFn: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				go func() {
					time.Sleep(time.Second * 11)
					cancel()
				}()
				return ctx
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			td := tc.mock(ctrl)

			preempter := NewDefaultPreempter(td)
			l, err := preempter.Preempt(context.Background())
			assert.NoError(t, err)

			ctxFn := tc.ctxFn()
			sch := l.AutoRefresh(ctxFn)
			var shouldContinue = true
			for shouldContinue {
				select {
				case s, ok := <-sch:
					if ok && s.Err() != nil {
						assert.Equal(t, tc.wantErr, s.Err())
						err1 := l.Release(ctxFn)
						assert.NoError(t, err1)
						shouldContinue = false
					}
				}
			}
		})
	}
}

func TestPreemptScheduler_TaskLeaser_Release(t *testing.T) {
	testCases := []struct {
		name    string
		mock    func(ctrl *gomock.Controller) storage.TaskRepository
		wantErr error
		ctxFn   func() context.Context
	}{
		{
			name: "正常结束（release）",
			mock: func(ctrl *gomock.Controller) storage.TaskRepository {
				td := daomocks.NewMockTaskRepository(ctrl)

				t := task.Task{
					ID:    1,
					Owner: "tom",
				}
				td.EXPECT().TryPreempt(gomock.Any(), gomock.Any()).Return(t, nil)

				td.EXPECT().RefreshTask(gomock.Any(), t.ID, t.Owner).AnyTimes().Return(nil)
				td.EXPECT().ReleaseTask(gomock.Any(), t.ID, t.Owner).AnyTimes().Return(nil)

				return td
			},
			wantErr: ErrLeaserHasRelease,
			ctxFn: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				go func() {
					time.Sleep(time.Second * 60)
					cancel()
				}()
				return ctx
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			td := tc.mock(ctrl)

			preempter := NewDefaultPreempter(td)
			l, err := preempter.Preempt(context.Background())
			assert.NoError(t, err)

			ctxFn := tc.ctxFn()
			sch := l.AutoRefresh(ctxFn)
			go func() {
				var shouldContinue = true
				for shouldContinue {
					select {
					case s, ok := <-sch:
						if ok && s.Err() != nil {
							assert.Equal(t, tc.wantErr, s.Err())
							err1 := l.Release(ctxFn)
							assert.NoError(t, err1)
							shouldContinue = false
						}
					}
				}
			}()
			time.Sleep(time.Second * 6)
			err1 := l.Release(ctxFn)
			assert.NoError(t, err1)

			time.Sleep(time.Second * 12)
			select {
			case _, ok := <-sch:
				assert.Equal(t, ok, false)
			}

		})
	}
}
