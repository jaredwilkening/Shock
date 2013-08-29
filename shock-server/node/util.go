package node

import ()

func contains(list []string, elem string) bool {
	for _, t := range list {
		if t == elem {
			return true
		}
	}
	return false
}

var (
	LockMgr = NewLocker()
)

type Locker struct {
	partLock chan bool //semaphore for checkout (mutual exclusion between different clients)
}

func NewLocker() *Locker {
	return &Locker{
		partLock: make(chan bool, 1), //non-blocking buffered channel
	}
}

func (l *Locker) LockPartOp() {
	l.partLock <- true
}

func (l *Locker) UnlockPartOp() {
	<-l.partLock
}
