package tasqueue

var (
	concurrencyOpt = "concurrency"
	customQueueOpt = "custom_queue"
	customMaxRetry = "custom_max_retry"
	customSchedule = "custom_schedule"
)

// ConcurrencyOpt defines an option to set concurrency for workers.
type ConcurrencyOpt uint32

func Concurrency(val uint32) ConcurrencyOpt {
	return ConcurrencyOpt(val)
}

func (c ConcurrencyOpt) Name() string       { return concurrencyOpt }
func (c ConcurrencyOpt) Value() interface{} { return uint32(c) }

// CustomQueue defines a custom queue for the task.
type CustomQueue string

func Queue(name string) Opts {
	return CustomQueue(name)
}

func (c CustomQueue) Name() string       { return customQueueOpt }
func (c CustomQueue) Value() interface{} { return string(c) }

// CustomSchedule defines a custom queue for the task.
type CustomSchedule string

func Schedule(spec string) Opts {
	return CustomSchedule(spec)
}

func (c CustomSchedule) Name() string       { return customSchedule }
func (c CustomSchedule) Value() interface{} { return string(c) }

// CustomMaxRetry defines a custom value for max retries for a task.
type CustomMaxRetry uint32

func MaxRetry(val uint32) Opts {
	return CustomMaxRetry(val)
}

func (c CustomMaxRetry) Name() string       { return customMaxRetry }
func (c CustomMaxRetry) Value() interface{} { return uint32(c) }
