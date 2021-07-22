package main

import (
  "context"
  "fmt"
  "time"
  "os"
  "os/signal"
  
  //"github.com/Azure/azure-event-hubs-go" 
  //"github.com/Azure/azure-event-hubs-go/v2" 
  "github.com/Azure/azure-event-hubs-go/v3"

  "net/http"
  "log"
) 
import _ "net/http/pprof"

func main() {

  go func() {
    log.Println(http.ListenAndServe("localhost:6061", nil))
  }()

  connStr := os.Getenv("EVENTHUB_CONNECTION_STRING1")
  hub, err := eventhub.NewHubFromConnectionString(connStr)
  if err != nil {
    // handle err
    fmt.Println(err)
  }

  ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
  defer cancel()

  handler := func(c context.Context, event *eventhub.Event) error {
    fmt.Println(string(event.Data))
    return nil
  }

  // listen to each partition of the Event Hub
  runtimeInfo, err := hub.GetRuntimeInformation(ctx)
  if err != nil {
    // handle err
  }
  
  for _, partitionID := range runtimeInfo.PartitionIDs { 
    // Start receiving messages 
    // 
    // Receive blocks while attempting to connect to hub, then runs until listenerHandle.Close() is called 
    // <- listenerHandle.Done() signals listener has died 
    // listenerHandle.Err() provides the last error the receiver encountered 
    //listenerHandle, err := hub.Receive(ctx, partitionID, handler, eventhub.ReceiveWithLatestOffset())
    _, err := hub.Receive(ctx, partitionID, handler, eventhub.ReceiveWithLatestOffset())
    if err != nil {
      // handle err
      fmt.Println("Error: ", err)
      return
    }
  }
  cancel()

  fmt.Println("Listening..")

  // Wait for a signal to quit:
  signalChan := make(chan os.Signal, 1)
  signal.Notify(signalChan, os.Interrupt, os.Kill)
  <-signalChan

  hub.Close(context.Background())
}