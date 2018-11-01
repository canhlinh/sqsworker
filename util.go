package sqsworker

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// CreateSqsQueueIfNotExist create sqs if not exist
func CreateSqsQueueIfNotExist(sqsClient *sqs.SQS, queue string) (*string, error) {
	sqsQueue, err := sqsClient.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(queue),
	})

	if err != nil {
		return CreateSqsQueue(sqsClient, queue)
	}

	return sqsQueue.QueueUrl, nil
}

// CreateSqsQueue create SQS queue
func CreateSqsQueue(sqsClient *sqs.SQS, queue string) (*string, error) {
	sqsQueue, err := sqsClient.CreateQueue(&sqs.CreateQueueInput{
		QueueName: aws.String(queue),
		Attributes: map[string]*string{
			"FifoQueue":                 aws.String("true"),
			"ContentBasedDeduplication": aws.String("true"),
		},
	})
	if err != nil {
		return nil, err
	}

	return sqsQueue.QueueUrl, nil
}

// DeleteSqsQueue deletes a sqs queue
func DeleteSqsQueue(sqsClient *sqs.SQS, queue string) error {
	sqsQueue, err := sqsClient.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(queue),
	})

	if err != nil {
		return nil
	}

	_, err = sqsClient.DeleteQueue(&sqs.DeleteQueueInput{
		QueueUrl: sqsQueue.QueueUrl,
	})

	return err

}
