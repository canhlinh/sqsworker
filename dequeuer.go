package sqsworker

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

//  DefaultWaitTime default seconds for wating to receive jobs from the queue
var DefaultWaitTime int64 = 10

const (
	MaxNumberOfMessages = 10
)

// Dequeuer present a dequeuer
type Dequeuer struct {
	queueURL       *string
	sqsClient      *sqs.SQS
	visibleTimeout int64
	maxJobs        int64
	cancelFunc     context.CancelFunc
	muxCancelFunc  *sync.Mutex
}

// NewDequeuer create a new dequeuer
func NewDequeuer(queueURL *string, sqsClient *sqs.SQS, visibleTimeout int64, maxJobs int64) *Dequeuer {
	if maxJobs > MaxNumberOfMessages {
		maxJobs = MaxNumberOfMessages
	}

	return &Dequeuer{
		queueURL:       queueURL,
		sqsClient:      sqsClient,
		visibleTimeout: visibleTimeout,
		maxJobs:        maxJobs,
		muxCancelFunc:  &sync.Mutex{},
	}
}

// Poll polls new jobs in the sqs queue
func (d *Dequeuer) Poll() ([]*Job, error) {
	msgs, err := d.pollMessage()
	if err != nil {
		return nil, err
	}

	jobs := []*Job{}
	for _, msg := range msgs {
		job, err := SqsMessageToJob(msg)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}

	return jobs, nil
}

// Cancel stops polling message from SQS
func (d *Dequeuer) Cancel() {
	d.muxCancelFunc.Lock()
	defer d.muxCancelFunc.Unlock()

	if d.cancelFunc != nil {
		d.cancelFunc()
		d.cancelFunc = nil
	}
}

func (d *Dequeuer) newContext() context.Context {
	d.muxCancelFunc.Lock()
	defer d.muxCancelFunc.Unlock()

	ctx, cancelFunc := context.WithCancel(context.TODO())
	d.cancelFunc = cancelFunc
	return ctx
}

func (d *Dequeuer) pollMessage() ([]*sqs.Message, error) {
	ctx := d.newContext()
	defer d.Cancel()

	receiverMessage, err := d.sqsClient.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:              d.queueURL,
		VisibilityTimeout:     aws.Int64(d.visibleTimeout),
		WaitTimeSeconds:       aws.Int64(20),
		MaxNumberOfMessages:   aws.Int64(d.maxJobs),
		AttributeNames:        []*string{aws.String("All")},
		MessageAttributeNames: []*string{aws.String(JobNameAttr)},
	})

	if err != nil {
		return nil, err
	}

	return receiverMessage.Messages, nil
}

// Dequeue removes a job out of queue
func (d *Dequeuer) Dequeue(job *Job) error {

	if _, err := d.sqsClient.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      d.queueURL,
		ReceiptHandle: job.ReceiptID,
	}); err != nil {
		return err
	}

	return nil
}
