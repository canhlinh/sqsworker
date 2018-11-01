package sqsworker

import (
	"encoding/json"
	"errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/pborman/uuid"
)

// Job represents a job.
type Job struct {
	ID        *string
	Name      string
	ReceiptID *string
	Args      map[string]interface{}
	Retries   int
}

// JobHandler signature function to consume a job
type JobHandler func(args map[string]interface{}) error

// SqsMessageToJob converts sqs message to a job
func SqsMessageToJob(msg *sqs.Message) (*Job, error) {
	jobNameAttr, ok := msg.MessageAttributes[JobNameAttr]
	if !ok {
		return nil, errors.New("Missing MessageAttributes job_name")
	}

	var args map[string]interface{}
	if err := json.Unmarshal([]byte(aws.StringValue(msg.Body)), &args); err != nil {
		return nil, err
	}

	job := &Job{
		ID:        msg.MessageId,
		ReceiptID: msg.ReceiptHandle,
		Name:      aws.StringValue(jobNameAttr.StringValue),
		Args:      args,
	}
	return job, nil
}

// SqsMessage convert Job to SendMessageInput
func (job *Job) SqsMessage() (*sqs.SendMessageInput, error) {
	d, err := json.Marshal(job.Args)
	if err != nil {
		return nil, err
	}
	body := string(d)

	return &sqs.SendMessageInput{
		MessageBody:    aws.String(body),
		MessageGroupId: aws.String(uuid.New()),
	}, nil
}

// MapToJSON marshal a map to json string
func MapToJSON(m map[string]interface{}) string {
	d, _ := json.Marshal(m)
	return string(d)
}
