package preempt

type DefaultStatus struct {
	err error
}

func (d DefaultStatus) Err() error {
	return d.err
}

func NewDefaultStatus(err error) DefaultStatus {
	return DefaultStatus{
		err: err,
	}
}
