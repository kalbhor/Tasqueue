<a href="https://zerodha.tech"><img src="https://zerodha.tech/static/images/github-badge.svg" align="right" /></a>

![taskqueue](https://user-images.githubusercontent.com/14031096/170992942-3b62e055-6d9e-4c08-a277-ed6d6e9a4c2a.png)

[![GoDoc](https://godoc.org/github.com/kalbhor/tasqueue?status.svg)](https://godoc.org/github.com/kalbhor/tasqueue) 

**Tasqueue** is a simple, lightweight distributed job/worker implementation in Go

* [Concepts](#concepts)
* [Server](#server)
	* [Options](#server-options)
	* [Usage](#usage)
	* [Task Options](#task-options)
	* [Registering Tasks](#registering-tasks)
	* [Starting Server](#start-server)
* [Job](#job)
	* [Options](#job-options)
	* [Creating a job](#creating-a-job)
	* [Enqueuing a job](#enqueuing-a-job)
	* [Getting job message](#getting-a-job-message)
* [Group](#group)
	* [Creating a group](#creating-a-group)
	* [Enqueuing a group](#enqueuing-a-group)
	* [Getting group message](#getting-a-group-message)
* [Chain](#chain)
	* [Creating a chain](#creating-a-chain)



## Concepts 
- `tasqueue.Broker` is a generic interface to enqueue and consume messages from a single queue. Currently supported brokers are
[redis](./brokers/redis/) and [nats-jetstream](./brokers/nats-js/).
- `tasqueue.Results` is a generic interface to store the status and results of jobs. Currently supported result stores are
[redis](./results/redis/) and [nats-jetstream](./results/nats-js/).
- `tasqueue.Task` is a pre-registered job handler. It stores a handler functions which is called to process a job. It also stores callbacks (if set through options), executed during different states of a job.
- `tasqueue.Job` represents a unit of work pushed to a queue for consumption. It holds:
	- `[]byte` payload (encoded in any manner, if required)
	- task name used to identify the pre-registed task which will processes the job.

### Server
A tasqueue server is the main store that holds the broker and the results interfaces. It also acts as a hub to register tasks.

#### Server Options
- `func Queue(string) Opts` : Name of the queue to enqueue/consume jobs from. (default: `tasqueue:tasks`)
- `func Concurrency(uint32) Opts` : The number of go routines spawned to process jobs on the registered queue. (default : `1`)

#### Usage
```go
package main

import (
	"log"

	"github.com/kalbhor/tasqueue"
	redis_broker "github.com/kalbhor/tasqueue/brokers/redis"
	redis_results "github.com/kalbhor/tasqueue/results/redis"
)

func main() {
	broker := redis_broker.New(redis_broker.Options{
		Addrs:    []string{"127.0.0.1:6379"},
		Password: "",
		DB:       0,
	})
	results := redis_results.New(redis_results.Options{
		Addrs:    []string{"127.0.0.1:6379"},
		Password: "",
		DB:       0,
	})


	srv, err := tasqueue.NewServer(broker, results, tasqueue.Concurrency(5))
	if err != nil {
		log.Fatal(err)
	}
}
```

#### Task Options 
- `func SuccessCallback(f func(JobCtx)) Opts` : Callback function executed when a job completes successfully.

- `func ProcessingCallback(f func(JobCtx)) Opts` : Callback function executed when a job is being processed (picked up by processor).

- `func RetryingCallback(f func(JobCtx)) Opts` : Callback function executed when a job fails and is enqueued to be retried. 

- `func FailureCallback(f func(JobCtx)) Opts` : Callback function executed when a job fails (all retries complete).

#### Registering tasks
```go
type SumPayload struct {
	Arg1 int `json:"arg1"`
	Arg2 int `json:"arg2"`
}

type SumResult struct {
	Result int `json:"result"`
}

// SumProcessor saves the sum of two integer arguements.
func SumProcessor(b []byte, m tasqueue.JobCtx) error {
	var pl SumPayload
	if err := json.Unmarshal(b, &pl); err != nil {
		return err
	}

	rs, err := json.Marshal(SumResult{Result: pl.Arg1 + pl.Arg2})
	if err != nil {
		return err
	}

	m.Save(rs)

	return nil
}
```

```go
srv.RegisterTask("add", tasks.SumProcessor)
```

#### Start server
Start() starts the job consumer and processor. It is a blocking function. It listens for jobs on the queue and spawns processor go routines.

```go
srv.Start(context.Background())
```

### Job
A tasqueue job represents a unit of work pushed onto the queue, that requires processing using a registered Task. It holds a `[]byte` payload, a task name (which will process the payload) and various options.

#### Job Options 
- `func Queue(string) Opts` : Name of the queue onto which the job is pushed. (default: `tasqueue:tasks`)
* Note : To consume jobs pushed on a custom queue, the server must be initialised with the same queue option.
- `func Schedule(string) Opts`: Cron schedule for the job
- `func MaxRetry(uint32) Opts`: Number of times the job will be retried, if failed. (default: `1`)

#### Creating a job
`NewJob` returns a job with arbitrary payload. It accepts the name of the task, the payload and a list of options.

```go
b, _ := json.Marshal(tasks.SumPayload{Arg1: 5, Arg2: 4})
job, err := tasqueue.NewJob("add", b)
if err != nil {
	log.Fatal(err)
}
```

#### Enqueuing a job
Once a job is created, it can be enqueued via the server for processing. Calling `srv.Enqueue` returns a job uuid which can be used to query the status of the job.

```go
uuid, err := srv.Enqueue(ctx, job)
if err != nil {
	log.Fatal(err)
}
```

#### Getting a job message 
To query the details of a job that was enqueued, we can use `srv.GetJob`. It returns a `JobMessage` which contains details related to a job. 

```go
jobMsg, err := srv.GetJob(ctx, uuid)
if err != nil {
	log.Fatal(err)
}
```
Fields available in a `JobMessage` (embeds `Meta`):
```go
// Meta contains fields related to a job. These are updated when a task is consumed.
type Meta struct {
	UUID        string
	Status      string
	Queue       string
	Schedule    string
	MaxRetry    uint32
	Retried     uint32
	PrevErr     string
	ProcessedAt time.Time
}
```

### Group
A tasqueue group holds multiple jobs and pushes them all simultaneously onto the queue, the Group is considered successful only if all the jobs finish successfully.

#### Creating a group
`NewGroup` returns a Group holding the jobs passed.

```go
var group []tasqueue.Job

for i := 0; i < 3; i++ {
	b, _ := json.Marshal(tasks.SumPayload{Arg1: i, Arg2: 4})
	job, err := tasqueue.NewJob("add", b)
	if err != nil {
			log.Fatal(err)
	}
	group = append(group, job)
}

grp, err := tasqueue.NewGroup(group...)
if err != nil {
	log.Fatal(err)
}
```

#### Enqueuing a group
Once a group is created, it can be enqueued via the server for processing. Calling `srv.EnqueueGroup` returns a group uuid which can be used to query the status of the group.

```go
groupUUID, err := srv.EnqueueGroup(ctx, grp)
if err != nil {
	log.Fatal(err)
}
```

#### Getting a group message 
To query the details of a group that was enqueued, we can use `srv.GetGroup`. It returns a `GroupMessage` which contains details related to a group. 

```go
groupUUID, err := srv.EnqueueGroup(ctx, grp)
if err != nil {
	log.Fatal(err)
}
```
Fields available in a `GroupMessage` (embeds `GroupMeta`):
```go
// GroupMeta contains fields related to a group job. These are updated when a task is consumed.
type GroupMeta struct {
	UUID   string
	Status string
	// JobStatus is a map of individual job uuid -> status
	JobStatus map[string]string
}
```

### Chain
A tasqueue chain holds multiple jobs and pushes them one after the other (after a job succeeds), the Chain is considered successful only if the final job completes successfuly.

#### Creating a chain
`NewChain` returns a chain holding the jobs passed in the order.

```go
var chain []tasqueue.Job

for i := 0; i < 3; i++ {
	b, _ := json.Marshal(tasks.SumPayload{Arg1: i, Arg2: 4})
	task, err := tasqueue.NewJob("add", b)
	if err != nil {
		log.Fatal(err)
	}
	chain = append(chain, task)
}

chn, err := tasqueue.NewChain(chain...)
if err != nil {
	log.Fatal(err)
}
```

## Credits 
- [@knadh](github.com/knadh) for the logo, code review & suggestions - callbacks, `JobCtx`

## License 
BSD-2-Clause-FreeBSD
