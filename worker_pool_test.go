package sqsworker

import (
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/pborman/uuid"
	. "github.com/smartystreets/goconvey/convey"
)

func wait(wg *sync.WaitGroup) chan bool {
	ch := make(chan bool)
	go func() {
		wg.Wait()
		ch <- true
	}()
	return ch
}

func getTestQueue() string {
	return uuid.New() + ".fifo"
}

func TestNew(t *testing.T) {
	Convey("Should create new SqsWorkerPool success", t, func() {
		sqstestqueue := getTestQueue()

		session, err := session.NewSession()
		So(err, ShouldBeNil)

		sqsClient := sqs.New(session, aws.NewConfig().WithRegion("us-east-1"))
		defer DeleteSqsQueue(sqsClient, sqstestqueue)

		sqsWorkerPool := New(sqstestqueue, 10, sqsClient)
		So(sqsWorkerPool, ShouldNotBeNil)
		So(sqsWorkerPool.Enqueuer, ShouldNotBeNil)
		So(sqsWorkerPool.Dequeuer, ShouldNotBeNil)
		So(sqsWorkerPool.maxJobs, ShouldEqual, 10)
	})
}

func TestRegisterJobHandler(*testing.T) {
}

func TestEnqueue(t *testing.T) {
	Convey("Enqueue Job", t, func() {
		session := session.Must(session.NewSession())
		sqsClient := sqs.New(session, aws.NewConfig().WithRegion("us-east-1"))
		sqstestqueue := getTestQueue()
		defer DeleteSqsQueue(sqsClient, sqstestqueue)

		Convey("Should be succeeded if queue job has matched with a registed handler", func() {
			sqsWorkerPool := New(sqstestqueue, 10, sqsClient)
			sqsWorkerPool.RegisterJobHandler("name1", func(args map[string]interface{}) error {
				return nil
			})

			params := map[string]interface{}{"a": 1}
			job, err := sqsWorkerPool.Enqueue("name1", params)
			So(err, ShouldBeNil)
			So(job, ShouldNotBeNil)
			So(job.Name, ShouldEqual, "name1")
			So(job.Args, ShouldEqual, params)
			defer sqsWorkerPool.Dequeue(job)
		})

		Convey("Should be failed if queue job has not matched with a registed handler", func() {
			sqsWorkerPool := New(sqstestqueue, 10, sqsClient)
			sqsWorkerPool.RegisterJobHandler("name1", func(args map[string]interface{}) error {
				return nil
			})
			_, err := sqsWorkerPool.Enqueue("name2", nil)
			So(err, ShouldNotBeNil)
		})
	})
}

func TestStart(t *testing.T) {
	Convey("Start WorkerPool", t, func() {
		session := session.Must(session.NewSession())
		sqsClient := sqs.New(session, aws.NewConfig().WithRegion("us-east-1"))
		sqstestqueue := getTestQueue()
		defer DeleteSqsQueue(sqsClient, sqstestqueue)

		Convey("Should success if a WorkerPool setup correctly", func() {
			sqsWorkerPool := New(sqstestqueue, 10, sqsClient)
			sqsWorkerPool.RegisterJobHandler("name", func(args map[string]interface{}) error {
				return nil
			})
			So(sqsWorkerPool.Start, ShouldNotPanic)
			So(sqsWorkerPool.jobChannel, ShouldNotBeNil)
			So(sqsWorkerPool.jobHandlers, ShouldNotBeNil)
			So(sqsWorkerPool.wgWorkers, ShouldNotBeNil)

			sqsWorkerPool.Stop()
			So(func() { close(sqsWorkerPool.jobChannel) }, ShouldPanicWith, "close of closed channel")
		})

		Convey("Should panic if start a WorkerPool has no job handlers", func() {

			sqsWorkerPool := New(sqstestqueue, 10, sqsClient)
			So(sqsWorkerPool.Start, ShouldPanicWith, "There is no job handler has been registered")
		})

		Convey("Should panic if start a WorkerPool that has already started", func() {

			sqsWorkerPool := New(sqstestqueue, 10, sqsClient)
			sqsWorkerPool.RegisterJobHandler("name", func(args map[string]interface{}) error {
				return nil
			})
			So(sqsWorkerPool.Start, ShouldNotPanic)
			defer sqsWorkerPool.Stop()
			So(sqsWorkerPool.Start, ShouldPanicWith, "The worker pool has already been started")
		})
	})
}

func TestConsumeJobs(t *testing.T) {
	session := session.Must(session.NewSession())
	sqsClient := sqs.New(session, aws.NewConfig().WithRegion("us-east-1"))
	sqstestqueue := getTestQueue()
	defer DeleteSqsQueue(sqsClient, sqstestqueue)

	Convey("Consumes many jobs at the same time", t, func() {
		firstParam := uuid.New()
		numJobs := 10
		numWorkers := int64(2)

		wg := &sync.WaitGroup{}
		wg.Add(numJobs)
		jobHandler := func(args map[string]interface{}) error {
			wg.Done()
			if args["first"] != firstParam || args["second"] == nil || len(args) != 2 {
				t.Fatal("args not match")
			}
			return nil
		}

		jobName := uuid.New()
		sqsWorkerPool := New(sqstestqueue, numWorkers, sqsClient)
		sqsWorkerPool.RegisterJobHandler(jobName, jobHandler)
		sqsWorkerPool.Start()
		for i := 0; i < numJobs; i++ {
			go func(i int) {
				args := map[string]interface{}{
					"first":  firstParam,
					"second": i,
				}
				_, err := sqsWorkerPool.Enqueue(jobName, args)
				if err != nil {
					t.Fatal(err)
				}
			}(i)
		}

		completed := false
		select {
		case <-time.After(time.Second * time.Duration(numJobs)):
			completed = false
		case <-wait(wg):
			completed = true
		}
		So(completed, ShouldBeTrue)

		sqsWorkerPool.Stop()
		So(func() { close(sqsWorkerPool.jobChannel) }, ShouldPanicWith, "close of closed channel")
	})
}

func TestStop(t *testing.T) {
}
