<a href="https://zerodha.tech"><img src="https://zerodha.tech/static/images/github-badge.svg" align="right" /></a>

![taskqueue](https://user-images.githubusercontent.com/14031096/170992942-3b62e055-6d9e-4c08-a277-ed6d6e9a4c2a.png)

[![Run Tests](https://github.com/kalbhor/tasqueue/v2/actions/workflows/test.yml/badge.svg)](https://github.com/kalbhor/tasqueue/v2/actions/workflows/test.yml) [![Go Report Card](https://goreportcard.com/badge/github.com/kalbhor/tasqueue/v2)](https://goreportcard.com/report/github.com/kalbhor/tasqueue/v2)

**Tasqueue** is a simple, lightweight distributed job/worker implementation in Go

### Installation

`go get -u github.com/kalbhor/tasqueue/v2`

- [Concepts](#concepts)
- [Server](#server)
  - [Options](#server-options)
  - [Usage](#usage)
  - [Task Options](#task-options)
  - [Registering Tasks](#registering-tasks)
  - [Starting Server](#start-server)
- [Job](#job)
  - [Options](#job-options)
  - [Creating a job](#creating-a-job)
  - [Enqueuing a job](#enqueuing-a-job)
  - [Getting job message](#getting-a-job-message)
  - [JobCtx](#jobctx)
- [Group](#group)
  - [Creating a group](#creating-a-group)
  - [Enqueuing a group](#enqueuing-a-group)
  - [Getting group message](#getting-a-group-message)
- [Chain](#chain)
  - [Creating a chain](#creating-a-chain)
  - [Enqueuing a chain](#enqueuing-a-chain)
  - [Getting chain message](#getting-a-group-chain)
- [Result](#result)
  - [Get Result](#get-result)

## Concepts

- `tasqueue.Broker` is a generic interface to enqueue and consume messages from a single queue. Currently supported brokers are
  [redis](./brokers/redis/) and [nats-jetstream](./brokers/nats-js/). Note: It is important for the broker (or your enqueue, consume implementation) to guarantee atomicity. ie : Tasqueue does not provide locking capabilities to ensure unique job consumption.
- `tasqueue.Results` is a generic interface to store the status and results of jobs. Currently supported result stores are
  [redis](./results/redis/) and [nats-jetstream](./results/nats-js/).
- `tasqueue.Task` is a pre-registered job handler. It stores a handler functions which is called to process a job. It also stores callbacks (if set through options), executed during different states of a job.
- `tasqueue.Job` represents a unit of work pushed to a queue for consumption. It holds:
  - `[]byte` payload (encoded in any manner, if required)
  - task name used to identify the pre-registed task which will processes the job.

### Server

A tasqueue server is the main store that holds the broker and the results interfaces. It also acts as a hub to register tasks.

#### Server Options

Server options are used to configure the server. Broker & Results are mandatory, while logger and open telemetry provider are optional. Refer to the [in-memory](./examples/in-memory/main.go) example for an open telemetry implementation.

```go
type ServerOpts struct {
	// Mandatory results & broker implementations.
	Broker        Broker
	Results       Results

	// Optional logger and telemetry provider.
	Logger        logf.Logger
	TraceProvider *trace.TracerProvider
}
```

#### Usage

```go
package main

import (
	"log"

	"github.com/kalbhor/tasqueue/v2"
	rb "github.com/kalbhor/tasqueue/v2/brokers/redis"
	rr "github.com/kalbhor/tasqueue/v2/results/redis"
	"github.com/zerodha/logf"
)

func main() {
	lo := logf.New(logf.Opts{})

	broker := rb.New(rb.Options{
		Addrs:    []string{"127.0.0.1:6379"},
		Password: "",
		DB:       0,
	}, lo)
	results := rr.New(rr.Options{
		Addrs:    []string{"127.0.0.1:6379"},
		Password: "",
		DB:       0,
	}, lo)

	srv, err := tasqueue.NewServer(tasqueue.ServerOpts{
		Broker:        broker,
		Results:       results,
		Logger:        lo,
	})
	if err != nil {
		log.Fatal(err)
	}
}
```

#### Task Options
Queue is the name of the queue assigned to the task. By default the value is "tasqueue:tasks". Queues can be 
shared between tasks.

Concurrency defines the number of processor go-routines running on the queue. By default this number is equal
to `runtime.GOMAXPROCS(0)` (number of CPUs on the system). Ideally, it is recommended that the client tweak this number according
to their tasks.

```go
type TaskOpts struct {
	Concurrency  uint32
	Queue        string
	SuccessCB    func(JobCtx)
	ProcessingCB func(JobCtx)
	RetryingCB   func(JobCtx)
	FailedCB     func(JobCtx)
}
```

#### Registering tasks

A task can be registered by supplying a name, handler and options.
Jobs can be processed using a task registered by a particular name.
A handler is a function with the signature `func([]byte, JobCtx) error`. It is the responsibility of the handler to deal with the `[]byte` payload in whatever manner (decode, if required).

```go
package tasks

import (
	"encoding/json"

	"github.com/kalbhor/tasqueue/v2"
)

type SumPayload struct {
	Arg1 int `json:"arg1"`
	Arg2 int `json:"arg2"`
}

type SumResult struct {
	Result int `json:"result"`
}

// SumProcessor prints the sum of two integer arguements.
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

Once a queue is created if the client creates a task with an existing queue but supplies a different concurrency 
in the `TaskOpts`, then `RegisterTask` will return an error.

```go
// This creates the q1 queue if it doesn't exist and assigns 5 concurrency to it
err := srv.RegisterTask("add", tasks.SumProcessor, TaskOpts{Queue:"q1", Concurrency: 5})
if err != nil {
	log.Fatal(err)
}

// No error
err := srv.RegisterTask("div", tasks.DivProcessor, TaskOpts{Queue:"q1"})
if err != nil {
	log.Fatal(err)
}

// No error
err := srv.RegisterTask("sub", tasks.SubProcessor, TaskOpts{Queue:"q1", Concurrency: 5})
if err != nil {
	log.Fatal(err)
}

// This will return an error since q1 is already created and its concurrency cannot be modified
err := srv.RegisterTask("multiplication", tasks.MulProcessor, TaskOpts{Queue:"q1", Concurrency: 10})
if err != nil {
	log.Fatal(err)
}
```

#### Start server

`Start()` starts the job consumer and processor. It is a blocking function. It listens for jobs on the queue and spawns processor go routines.

```go
srv.Start(ctx)
```

### Job

A tasqueue job represents a unit of work pushed onto the queue, that requires processing using a registered Task. It holds a `[]byte` payload, a task name (which will process the payload) and various options.

#### Job Options

```go
// JobOpts holds the various options available to configure a job.
type JobOpts struct {
	// Optional ID passed by client. If empty, Tasqueue generates it.
	ID string

	Queue      string
	MaxRetries uint32
	Schedule   string
	Timeout    time.Duration
}
```

#### Creating a job

`NewJob` returns a job with the supplied payload. It accepts the name of the task, the payload and a list of options.

```go
b, _ := json.Marshal(tasks.SumPayload{Arg1: 5, Arg2: 4})
job, err := tasqueue.NewJob("add", b, tasqueue.JobOpts{})
if err != nil {
	log.Fatal(err)
}
```

#### Enqueuing a job

Once a job is created, it can be enqueued via the server for processing. Calling `srv.Enqueue` returns a job id which can be used to query the status of the job.

```go
id, err := srv.Enqueue(ctx, job)
if err != nil {
	log.Fatal(err)
}
```

#### Getting a job message

To query the details of a job that was enqueued, we can use `srv.GetJob`. It returns a `JobMessage` which contains details related to a job.

```go
jobMsg, err := srv.GetJob(ctx, id)
if err != nil {
	log.Fatal(err)
}
```

Fields available in a `JobMessage` (embeds `Meta`):

```go
// Meta contains fields related to a job. These are updated when a task is consumed.
type Meta struct {
	ID          string
	OnSuccessID string
	Status        string
	Queue         string
	Schedule      string
	MaxRetry      uint32
	Retried       uint32
	PrevErr       string
	ProcessedAt   time.Time

	// PrevJobResults contains any job result set by the previous job in a chain.
	// This will be nil if the previous job doesn't set the results on JobCtx.
	PrevJobResult []byte
}
```

#### JobCtx

`JobCtx` is passed to handler functions and callbacks. It can be used to view the job's meta information (`JobCtx` embeds `Meta`) and also to save arbitrary results for a job using `func (c *JobCtx) Save(b []byte) error`

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

grp, err := tasqueue.NewGroup(group, tasqueue.GroupOpts{})
if err != nil {
	log.Fatal(err)
}
```

#### Enqueuing a group

Once a group is created, it can be enqueued via the server for processing. Calling `srv.EnqueueGroup` returns a group id which can be used to query the status of the group.

```go
groupID, err := srv.EnqueueGroup(ctx, grp)
if err != nil {
	log.Fatal(err)
}
```

#### Getting a group message

To query the details of a group that was enqueued, we can use `srv.GetGroup`. It returns a `GroupMessage` which contains details related to a group.

```go
groupMsg, err := srv.GetGroup(ctx, groupID)
if err != nil {
	log.Fatal(err)
}
```

Fields available in a `GroupMessage` (embeds `GroupMeta`):

```go
// GroupMeta contains fields related to a group job. These are updated when a task is consumed.
type GroupMeta struct {
	ID   string
	Status string
	// JobStatus is a map of individual job id -> status
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

chn, err := tasqueue.NewChain(chain, tasqueue.ChainOpts{})
if err != nil {
	log.Fatal(err)
}
```

#### Enqueuing a chain

Once a chain is created, it can be enqueued via the server for processing. Calling `srv.EnqueueChain` returns a chain id which can be used to query the status of the chain.

```go
chainID, err := srv.EnqueueChain(ctx, chn)
if err != nil {
	log.Fatal(err)
}
```

#### Getting results of previous job in a chain

A job in the chain can access the results of the previous job in the chain by getting `JobCtx.Meta.PrevJobResults`. This will contain any job result saved by the previous job by `JobCtx.Save()`.

#### Getting a chain message

To query the details of a chain that was enqueued, we can use `srv.GetChain`. It returns a `ChainMessage` which contains details related to a chian.

```go
chainMsg, err := srv.GetChain(ctx, chainID)
if err != nil {
	log.Fatal(err)
}
```

Fields available in a `ChainMessage` (embeds `ChainMeta`):

```go
// ChainMeta contains fields related to a chain job.
type ChainMeta struct {
	ID string
	// Status of the overall chain
	Status string
	// ID of the current job part of chain
	JobID string
	// List of IDs of completed jobs
	PrevJobs []string
}
```

### Result

A result is arbitrary `[]byte` data saved by a handler or callback via `JobCtx.Save()`.

#### Get Result

If the `jobID` does not exist, `ErrNotFound` will be returned

```go 
b, err := srv.GetResult(ctx, jobID)
if err != nil {
	log.Fatal(err)
}
```

#### Delete Result

DeleteJob removes the job's saved metadata from the store

```go
err := srv.DeleteResult(ctx, jobID)
if err != nil {
	log.Fatal(err)
}
```

## Credits

- [@knadh](github.com/knadh) for the logo & feature suggestions

## License

BSD-2-Clause-FreeBSD
