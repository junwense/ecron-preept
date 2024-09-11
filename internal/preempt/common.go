package preempt

type DefaultLeaseStatus struct {
	err error
}

func (d DefaultLeaseStatus) Err() error {
	return d.err
}

func NewDefaultLeaseStatus(err error) DefaultLeaseStatus {
	return DefaultLeaseStatus{
		err: err,
	}
}
