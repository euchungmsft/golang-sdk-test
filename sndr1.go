package main

import (
	"context"
	"fmt"
	"log"
	"net/url"
//	"os"
	"time"

	"github.com/Azure/azure-storage-queue-go/azqueue"
)

func accountInfo() (string, string) {
	return os.Getenv("ACCOUNT_NAME1"), os.Getenv("ACCOUNT_KEY1")
}

func main() {

	accountName, accountKey := accountInfo()

	credential, err := azqueue.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Fatal(err)
	}

	p := azqueue.NewPipeline(credential, azqueue.PipelineOptions{})	
	u, _ := url.Parse(fmt.Sprintf("https://%s.queue.core.windows.net", accountName))
	serviceURL := azqueue.NewServiceURL(*u, p)
	ctx := context.TODO() // This example uses a never-expiring context.
	queueURL := serviceURL.NewQueueURL("q001") // Queue names require lowercase

	_, err = queueURL.Create(ctx, azqueue.Metadata{})
	if err != nil {
		log.Fatal(err)
	}

	messagesURL := queueURL.NewMessagesURL()

	_, err = messagesURL.Enqueue(ctx, "This is message 1", time.Second*0, time.Minute)
	if err != nil {
		log.Fatal(err)
	}

}	