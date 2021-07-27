// for test
// curl -v -H "Content-Type: application/json" --data '{"name":"hello world"}' http://localhost:6667/event

/*
required variables

export ACCOUNT_NAME1=<Storage account name>
export ACCOUNT_KEY1=<Storage account key>
export QUEUE_NAME1=<Storage queue name>
export MAX_RETRIES1=<Maximum retries>
export RETRY_SLEEP1=<Retries sleeps>
export WORKER_COUNT1=<Woker thread count>
export EVENTHUB_CONNECTION_STRING1=<Event Hub connection string>
export CTX_TIMEOUTSEC1=<Context timeout in seconds>
*/

package main

import (
    "os"
    "context"
    "fmt"
    "log"
    "net/http"
    "io/ioutil"
    "time"
    "strconv"
    "encoding/json"
    "github.com/gorilla/mux"
    eventhub "github.com/Azure/azure-event-hubs-go/v3"
    //eventhub "github.com/Azure/azure-event-hubs-go"

    "sync"
    queue "github.com/sheerun/queue"    
    "net/url"
    azqueue "github.com/Azure/azure-storage-queue-go/azqueue"
)

import _ "net/http/pprof"

type App struct {
    Router         *mux.Router
    Hub            *eventhub.Hub
    MaxRetries     int
    RetrySleep     int
    ContextTimoutSec int
}

type Msg struct {
    Body string
    Retries int
}

var q1 = queue.New()
var q2 = queue.New()    //  deadlettering

func (a *App) Run(addr string) {
    log.Fatal(http.ListenAndServe(addr, a.Router))
}

func (a *App) Initialize(conn_str string) error {

    //a.Queue := make(chan time.Time, 10)

    // Connect postgres
    hub, err := eventhub.NewHubFromConnectionString(conn_str)
    if err != nil {
        //panic(err)
        log.Fatal(err.Error())
    }
    // Ping to connection
    
    // Set db in Model
    a.Hub = hub
    a.Router = mux.NewRouter()
    a.initializeRoutes()

    return nil
}

func (a *App) initializeRoutes() {
    a.Router.HandleFunc("/event", a.EventHandler).Methods("POST")
}

func respondWithJSON(w http.ResponseWriter, code int, payload interface{}) {
    // log.Println(payload)
    response, _ := json.Marshal(payload)

    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(code)
    w.Write(response)
}

func respondWithError(w http.ResponseWriter, code int, message string) {
    respondWithJSON(w, code, map[string]string{"error": message})
}

func (a *App) EventHandler(w http.ResponseWriter, r *http.Request) {
   //has access to DB
   
    body, err := ioutil.ReadAll(r.Body)
    if err != nil {
        //panic(err)
        log.Println(err.Error())
        respondWithJSON(w, http.StatusBadRequest, "Invalid request")
        return 
    }

    var m1 *Msg   
    m1 = new(Msg) 
    m1.Body = string(body)
    m1.Retries = 0
    q1.Append(m1)

    // fmt.Fprintf(w, "Welcome to the HomePage!")
    // fmt.Println("Endpoint Hit: homePage")
    respondWithJSON(w, http.StatusOK,string(body))
}

func worker(name string, wg *sync.WaitGroup, app *App) {
    wg.Done()

    log.Println(name, " started")   

    for {
        
        m2 := q1.Pop().(*Msg)
        body := m2.Body

        if m2.Retries >= app.MaxRetries {
            log.Println("Reached max retries, pushing to deadletter queue and skipping")
            q2.Append(m2)
            continue
        } else if m2.Retries > 0 {
            if app.RetrySleep > 0 {
                time.Sleep(time.Duration(app.RetrySleep) * time.Millisecond)
            }
        }

        //body := q1.Pop().(string)
        log.Println(fmt.Sprintf("%s, %v", name, body))
        //time.Sleep(10 * time.Millisecond)

        ctx, cancel := context.WithTimeout(context.Background(), time.Duration(app.ContextTimoutSec) * time.Second)
        //ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
        log.Println("Sending ", body, m2.Retries)
        err2 := app.Hub.Send(ctx, eventhub.NewEventFromString(body))
        if err2 != nil {
            log.Println(err2.Error(), " Retries")

            //// Breaks for errors
            //log.Println(err2.Error())
            //return

            //if err2 == context.DeadlineExceeded {
            //  log.Println("That error")
            //}

            m2.Retries += 1
            q1.Append(m2)
        }
        defer cancel()

        //time.Sleep(5 * time.Second)
        log.Println("Sent   ", body)   

    }
}

func dlworker(Context context.Context, MessagesURL azqueue.MessagesURL){

    log.Println("DL Worker started")   

    for {
        m3 := q2.Pop().(*Msg)
        body := m3.Body

        log.Println("Storing", body)
        _, err := MessagesURL.Enqueue(Context, body, time.Second*0, time.Minute)
        if err != nil {
            log.Println(err)
        }
    }

}

func initBlobQueue() (context.Context, azqueue.MessagesURL){

    log.Println("Initializing Blob Queue")   

    accountName := os.Getenv("ACCOUNT_NAME1")
    accountKey := os.Getenv("ACCOUNT_KEY1")
    log.Println("Account Name,", accountName);
    //log.Println("Account Key,", accountKey);
    credential, err := azqueue.NewSharedKeyCredential(accountName, accountKey)
    if err != nil {
        log.Fatal(err)
        //return
    }

    p := azqueue.NewPipeline(credential, azqueue.PipelineOptions{}) 
    u, _ := url.Parse(fmt.Sprintf("https://%s.queue.core.windows.net", accountName))
    serviceURL := azqueue.NewServiceURL(*u, p)
    context := context.TODO() // This example uses a never-expiring context.

    queueName := os.Getenv("QUEUE_NAME1")
    log.Println("Queue Name,", queueName);
    queueURL := serviceURL.NewQueueURL(queueName) // Queue names require lowercase
    _, err = queueURL.Create(context, azqueue.Metadata{})
    if err != nil {
        log.Fatal(err)
        //return
    }
    messagesURL := queueURL.NewMessagesURL()

    log.Println("Blob Queue's ready to ", accountName)   

    return context, messagesURL

}

func main() {

    go func() {
        log.Println(http.ListenAndServe(":6060", nil))
    }()

    configContextTimoutSec, err := strconv.Atoi(os.Getenv("CTX_TIMEOUTSEC1"))
    if err != nil {
        //log.Fatal(err.Error())
        log.Println("Invalid CTX_TIMEOUTSEC1, set as default 3 sec")
        configContextTimoutSec = 3
    }

    configMaxRetries, err := strconv.Atoi(os.Getenv("MAX_RETRIES1"))
    if err != nil {
        //log.Fatal(err.Error())
        log.Println("Invalid MAX_RETRIES1, set as default 3")
        configMaxRetries = 3
    }

    configRetrySleep, err := strconv.Atoi(os.Getenv("RETRY_SLEEP1"))
    if err != nil {
        //log.Fatal(err.Error())
        log.Println("Invalid RETRY_SLEEP1, set as default 1000 ms")
        configRetrySleep = 1000
    }

    configWorkerCount, err := strconv.Atoi(os.Getenv("WORKER_COUNT1"))
    if err != nil {
        log.Fatal(err.Error())
        return
    }

    var wg sync.WaitGroup
    wg.Add(configWorkerCount)

    connstr := os.Getenv("EVENTHUB_CONNECTION_STRING1")
    a := App{}
    a.MaxRetries = configMaxRetries
    a.RetrySleep = configRetrySleep
    a.ContextTimoutSec = configContextTimoutSec

    for i := 0; i < configWorkerCount; i++ {
        str := fmt.Sprintf("wrker %d", i)
        go worker(str, &wg, &a)
    }

    // Init URL, dlworker
    go dlworker(initBlobQueue())

    a.Initialize(connstr)
    a.Run(":6667")

    wg.Wait()

}