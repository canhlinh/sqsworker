package sqsworker

import (
	"log"
)

type Worker struct {
	Pool *WorkerPool
}

func NewWorker(pool *WorkerPool) *Worker {
	return &Worker{pool}
}

func (w *Worker) start() {
	w.Pool.wgWorkers.Add(1)

	go func() {
		defer w.Pool.wgWorkers.Done()
		for job := range w.Pool.jobChannel {
			w.process(job)
		}
	}()
	return
}

func (w *Worker) process(job *Job) {
	jobHandler := w.Pool.jobHandlers[job.Name]
	if err := jobHandler(job.Args); err == nil {
		if err := w.Pool.Dequeue(job); err != nil {
			log.Fatal("Failed to dequeue job ", err)
		}
	}
}
