package workers

import (
	"errors"
	"sync"
)

var (
	errTerminated = errors.New("terminated")
)

type Workers struct {
	quit      chan struct{}
	workersWg wgNilWrapper
	tasksWg   wgNilWrapper
	tasks     chan func()
}

func New(workersWg *sync.WaitGroup, tasksWg *sync.WaitGroup, quit chan struct{}, maxTasks int) *Workers {
	return &Workers{
		tasks:     make(chan func(), maxTasks),
		quit:      quit,
		workersWg: wgNilWrapper{workersWg},
		tasksWg:   wgNilWrapper{tasksWg},
	}
}

func (w *Workers) Start(workersN int) {
	for i := 0; i < workersN; i++ {
		w.workersWg.Add(1)
		go func() {
			defer w.workersWg.Done()
			w.worker()
		}()
	}
}

func (w *Workers) Enqueue(fn func()) error {
	w.tasksWg.Add(1)
	select {
	case w.tasks <- fn:
		return nil
	case <-w.quit:
		w.tasksWg.Done()
		return errTerminated
	}
}

func (w *Workers) Drain() {
	for {
		select {
		case <-w.tasks:
			w.tasksWg.Done()
			continue
		default:
			return
		}
	}
}

func (w *Workers) TasksCount() int {
	return len(w.tasks)
}

func (w *Workers) worker() {
	for {
		select {
		case <-w.quit:
			return
		case job := <-w.tasks:
			job()
			w.tasksWg.Done()
		}
	}
}

type wgNilWrapper struct {
	v *sync.WaitGroup
}

func (wg *wgNilWrapper) Add(delta int) {
	if wg.v == nil {
		return
	}
	wg.v.Add(delta)
}

func (wg *wgNilWrapper) Done() {
	if wg.v == nil {
		return
	}
	wg.v.Done()
}
