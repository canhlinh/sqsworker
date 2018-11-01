SqsWorker lets you enqueue and processes background backed by AWS SQS queue
==============================

[![Build Status](https://circleci.com/gh/canhlinh/sqsworker.svg?style=svg)](https://circleci.com/gh/canhlinh/sqsworker)
[![GoDoc](https://godoc.org/github.com/canhlinh/sqsworker?status.svg)](http://godoc.org/github.com/canhlinh/sqsworker)

**Features:**
- Consumes SQS messages as jobs by multi workers
- Enqueue jobs to as send messages to SQS queue

**Example:**

```
package main

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/pborman/uuid"
	"github.com/canhlinh/sqsworker"
)

func main() {
	testQueue := "testqueue.fifo"

	session := session.Must(session.NewSession())
	sqsClient := sqs.New(session, aws.NewConfig().WithRegion("us-east-1"))


	sqsWorkerPool := sqsworker.New(testQueue, 2, sqsClient)
	sqsWorkerPool.RegisterJobHandler("doingsomething", jobHandler)
	sqsWorkerPool.Start()
	sqsWorkerPool.Enqueue("doingsomething", map[string]interface{}{"paramter_a": uuid.New()})

	time.Sleep(3*time.Second)
	sqsWorkerPool.Stop()
}

 func jobHandler(args map[string]interface{}) error {
	fmt.Println("Run job", args)
	return nil
}
```