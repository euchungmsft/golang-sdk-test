// for test
// curl -v -H "Content-Type: application/json" --data '{"name":"hello world"}' http://localhost:6667/event

package main

import (
    "os"
    "context"
    "fmt"
    "log"
    "net/http"
    "io/ioutil"
    "time"
    "encoding/json"
    "github.com/gorilla/mux"
    eventhub "github.com/Azure/azure-event-hubs-go/v3"
    //eventhub "github.com/Azure/azure-event-hubs-go"

    "sync"
    queue "github.com/sheerun/queue"    
)

import _ "net/http/pprof"

const (
    MAX_DATA_SIZE = 100
)

type App struct {
    Router *mux.Router
    Hub     *eventhub.Hub;
}

var q1 = queue.New()

func (a *App) Run(addr string) {
    log.Fatal(http.ListenAndServe(addr, a.Router))
}

func (a *App) Initialize(conn_str string) error {

    //a.Queue := make(chan time.Time, 10)

    // Connect postgres
    hub, err := eventhub.NewHubFromConnectionString(conn_str)
    if err != nil {
        //panic(err)
        fmt.Println(err)
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
        fmt.Println(err)
    }

    q1.Append(string(body))

    /*
    ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
    log.Println("Sending", string(body))
    err2 := a.Hub.Send(ctx, eventhub.NewEventFromString(string(body)))
    if err2 != nil {
        log.Println(err2.Error())
        respondWithError(w, http.StatusInternalServerError, err2.Error())
        return
    }
    defer cancel()

    //time.Sleep(5 * time.Second)
    log.Println("Sent   ", string(body))
    */

    // fmt.Fprintf(w, "Welcome to the HomePage!")
    // fmt.Println("Endpoint Hit: homePage")
    respondWithJSON(w, http.StatusOK,string(body))
}

func worker(name string, wg *sync.WaitGroup, app *App) {
    wg.Done()

    for {
        body := q1.Pop().(string)
        fmt.Printf("%s, %v\n", name, body)
        //time.Sleep(10 * time.Millisecond)

        ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
        log.Println("Sending", string(body))
        err2 := app.Hub.Send(ctx, eventhub.NewEventFromString(string(body)))
        if err2 != nil {
            log.Println(err2.Error())
            //respondWithError(w, http.StatusInternalServerError, err2.Error())
            return
        }
        defer cancel()

        //time.Sleep(5 * time.Second)
        log.Println("Sent   ", string(body))        
    }
}

func main() {

    go func() {
        log.Println(http.ListenAndServe("localhost:6060", nil))
    }()

    var wg sync.WaitGroup
    wg.Add(10)

    connstr := os.Getenv("EVENTHUB_CONNECTION_STRING1")
    a := App{}

    for i := 0; i < 10; i++ {
        go worker("wrkr 1", &wg, &a)
    }

    a.Initialize(connstr)
    a.Run(":6667")

    wg.Wait()

}