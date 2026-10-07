package parallel

import (
	"container/heap"
	"sync"
	"sync/atomic"
	"time"
)

// scheduler hands tasks to a fixed set of workers, lowest position first, and
// reports each finished task on finished.
type scheduler struct {
	store    *store
	run      func(*Task) *Result
	finished chan *Task

	mu            sync.Mutex
	cond          *sync.Cond
	ready         taskHeap
	closed        bool
	executions    atomic.Int64
	executionTime atomic.Int64
	wg            sync.WaitGroup
}

func newScheduler(s *store, workers int, run func(*Task) *Result) *scheduler {
	sched := &scheduler{
		store: s,
		run:   run,
	}
	sched.cond = sync.NewCond(&sched.mu)
	sched.wg.Add(workers)
	for range workers {
		go sched.work()
	}
	return sched
}

// start releases every task that has no predecessor to wait for.
func (s *scheduler) start(tasks []*Task) {
	s.finished = make(chan *Task, len(tasks))
	for _, t := range tasks {
		if t.prev == nil {
			s.release(t)
		}
	}
}

// release marks t as released and pushes it to the
// ready queue if it hasnt been released before.
func (s *scheduler) release(t *Task) {
	t.mu.Lock()
	released := t.released
	t.released = true
	t.mu.Unlock()
	if !released {
		s.push(t)
	}
}

// stop lets running executions finish and stops the workers.
func (s *scheduler) stop() (int, time.Duration) {
	s.mu.Lock()
	s.closed = true
	s.ready = nil
	s.mu.Unlock()
	s.cond.Broadcast()
	s.wg.Wait()
	return int(s.executions.Load()), time.Duration(s.executionTime.Load())
}

func (s *scheduler) push(t *Task) {
	s.mu.Lock()
	heap.Push(&s.ready, t)
	s.mu.Unlock()
	s.cond.Signal()
}

func (s *scheduler) getTaskReadyForExecution() *Task {
	s.mu.Lock()
	defer s.mu.Unlock()
	for len(s.ready) == 0 && !s.closed {
		s.cond.Wait()
	}
	if s.closed {
		return nil
	}
	task := heap.Pop(&s.ready).(*Task)
	// check if the task has been released and marked as ready
	// for execution
	if !task.released {
		return nil
	}
	return task
}

func (s *scheduler) work() {
	defer s.wg.Done()
	for {
		t := s.getTaskReadyForExecution()
		if t == nil {
			return
		}
		started := time.Now()
		res := s.run(t)
		s.executions.Add(1)
		s.executionTime.Add(int64(time.Since(started)))
		s.finish(t, res)
	}
}

// finish publishes the result of t, reports it and releases the same-sender
// successor of t.
func (s *scheduler) finish(t *Task, res *Result) {
	t.mu.Lock()
	s.store.publish(t.Position, res.writes, res.codes)
	t.result = res
	t.mu.Unlock()
	s.finished <- t
	if t.next != nil {
		s.release(t.next)
	}
}

type taskHeap []*Task

func (h taskHeap) Len() int {
	return len(h)
}
func (h taskHeap) Less(i, j int) bool {
	return h[i].Position < h[j].Position
}
func (h taskHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}
func (h *taskHeap) Push(x any) {
	*h = append(*h, x.(*Task))
}

func (h *taskHeap) Pop() any {
	old := *h
	t := old[len(old)-1]
	*h = old[:len(old)-1]
	return t
}
