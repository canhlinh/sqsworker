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