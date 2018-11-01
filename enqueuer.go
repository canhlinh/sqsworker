package sqsworker

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/pborman/uuid"
)

const (
	JobNameAttr = "job_name"
)

// Enqueuer presents an queuer
type Enqueuer struct {
	queueURL  *string
	sqsClient *sqs.SQS
}

// NewEnqueuer creates new Enqueuer
func NewEnqueuer(queueURL *string, sqsClient *sqs.SQS) *Enqueuer {
	return &Enqueuer{queueURL, sqsClient}
}

// Enqueue enqueues a job
func (e *Enqueuer) Enqueue(jobName string, args map[string]interface{}) (*Job, error) {
	body := MapToJSON(args)

	input := &sqs.SendMessageInput{
		MessageBody:    aws.String(body),
		MessageGroupId: aws.String(uuid.New()),
		QueueUrl:       e.queueURL,
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			JobNameAttr: &sqs.MessageAttributeValue{
				DataType:    aws.String("String"),
				StringValue: aws.String(jobName),
			},
		},
	}

	output, err := e.sqsClient.SendMessage(input)
	if err != nil {
		return nil, err
	}

	return &Job{
		ID:   output.MessageId,
		Name: jobName,
		Args: args,
	}, nil
}
