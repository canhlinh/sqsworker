package sqsworker

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/service/sqs"
)

// WorkerPool represents a pool of workers
type WorkerPool struct {
	_ struct{}
	*Enqueuer
	*Dequeuer

	maxWorkers  int64
	jobHandlers map[string]JobHandler
	jobChannel  chan *Job
	wgWorkers   *sync.WaitGroup
}

// New create a new WorkerPool
func New(queue string, maxWorkers int64, sqsClient *sqs.SQS) *WorkerPool {
	if !strings.HasSuffix(queue, ".fifo") {
		panic("The queue should has suffix .fifo")
	}

	sqsQueueURL, err := CreateSqsQueueIfNotExist(sqsClient, queue)
	if err != nil {
		panic(err)
	}

	enqueuer := NewEnqueuer(sqsQueueURL, sqsClient)
	dequeuer := NewDequeuer(sqsQueueURL, sqsClient, DefaultWaitTime, maxWorkers)

	return &WorkerPool{
		Enqueuer:   enqueuer,
		Dequeuer:   dequeuer,
		maxWorkers: maxWorkers,
	}
}

// RegisterJobHandler register job handlers before start workers
func (pool *WorkerPool) RegisterJobHandler(name string, handler JobHandler) {
	if pool.jobChannel != nil {
		panic("Can not register new job hanlder after started the workers")
	}

	if pool.jobHandlers == nil {
		pool.jobHandlers = make(map[string]JobHandler)
	}
	pool.jobHandlers[name] = handler
}

// Start start the worker pool
func (pool *WorkerPool) Start() {
	if pool.jobHandlers == nil {
		panic("There is no job handler has been registered")
	}
	if pool.jobChannel != nil {
		panic("The worker pool has already been started")
	}

	pool.wgWorkers = &sync.WaitGroup{}
	pool.jobChannel = make(chan *Job, pool.maxWorkers)

	for i := int64(0); i < pool.maxWorkers; i++ {
		worker := &Worker{pool}
		worker.start()
	}

	go func() {
		defer pool.destroy()

		for {
			jobs, err := pool.Dequeuer.Poll()
			if err != nil {
				if !strings.Contains(err.Error(), "context canceled") {
					log.Fatal(err)
				}
				return
			}

			for _, job := range jobs {
				pool.jobChannel <- job
			}
		}
	}()
}

// Stop the worker pool
func (pool *WorkerPool) Stop() {
	pool.Dequeuer.Cancel()
	pool.wgWorkers.Wait()
}

func (pool *WorkerPool) destroy() {
	close(pool.jobChannel)
}

// ExistJobHandler check whether a job handler has been registered
func (pool *WorkerPool) ExistJobHandler(name string) bool {
	_, ok := pool.jobHandlers[name]
	return ok
}

// Enqueue enqueues a new job
func (pool *WorkerPool) Enqueue(name string, args map[string]interface{}) (*Job, error) {
	if name == "" {
		return nil, errors.New("JobName can not be empty")
	}

	if !pool.ExistJobHandler(name) {
		return nil, fmt.Errorf("Job name %s has no JobHandler", name)
	}

	return pool.Enqueuer.Enqueue(name, args)
}
