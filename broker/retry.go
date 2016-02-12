package main

import (
	"log"
	"time"
)

// RetryQueue implements PriorityQueue using Go's built-in heap
// priorty is set based on Job's retryAtTime field
type RetryQueue []*Job

func (rq RetryQueue) Len() int { return len(rq) }

func (rq RetryQueue) Less(i, j int) bool {
	return rq[i].retryAtTime.Before(rq[j].retryAtTime)
}

func (rq RetryQueue) Swap(i, j int) {
	rq[i], rq[j] = rq[j], rq[i]
}

func (rq *RetryQueue) Push(x interface{}) {
	*rq = append(*rq, x.(*Job))
}

func (rq *RetryQueue) Pop() interface{} {
	old := *rq
	n := len(old)
	item := old[n-1]
	*rq = old[0 : n-1]
	return item
}

func (rq *RetryQueue) Peek() interface{} {
	if rq.Len() > 0 {
		return (*rq)[0]
	} else {
		return nil
	}
}

// RetryTimer implements a dynamic timer that can be set by duration or time
type RetryTimer struct {
	timer     *time.Timer
	isRunning bool
	cancel    chan bool
	retryTime time.Time
	ch        chan time.Time
	Event     <-chan time.Time
}

func NewRetryTimer() *RetryTimer {
	t := &RetryTimer{retryTime: time.Now(), isRunning: false, ch: make(chan time.Time), cancel: make(chan bool)}
	t.Event = t.ch
	return t
}

func (r *RetryTimer) start() {
	select {
	case now := <-r.timer.C:
		r.ch <- now
		r.isRunning = false
	case <-r.cancel:
		r.isRunning = false
	}
}

func (r *RetryTimer) IsRunning() bool {
	return r.isRunning
}

func (r *RetryTimer) SetDuration(d time.Duration) {
	if d > 0 {
		r.Stop()
		r.retryTime = time.Now().Add(d)
		r.timer = time.NewTimer(d)
		r.isRunning = true
		go r.start()
	} else {
		log.Println("Invalid RetryTimer Duration", d)
	}
}

func (r *RetryTimer) SetTime(to time.Time) {
	if to.After(time.Now()) {
		r.SetDuration(to.Sub(time.Now()))
	} else {
		log.Println("Invalid RetryTimer Time", to)
	}
}

func (r *RetryTimer) RetryTime() time.Time {
	return r.retryTime
}

func (r *RetryTimer) Stop() bool {
	if r.isRunning {
		r.cancel <- true
		return r.timer.Stop()
	} else {
		return false
	}
}
