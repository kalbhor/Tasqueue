package tasqueue

var (
	concurrencyOpt = "concurrency"
	customQueueOpt = "custom_queue"
)

// ConcurrencyOpt defines an option to set concurrency for workers.
type ConcurrencyOpt uint32

func Concurrency(val uint32) ConcurrencyOpt {
	return ConcurrencyOpt(val)
}

func (c ConcurrencyOpt) Name() string       { return concurrencyOpt }
func (c ConcurrencyOpt) Value() interface{} { return uint32(c) }

// CustomQueue implements TaskOpts and defines a custom queue for the task.
type CustomQueue string

func Queue(name string) Opts {
	return CustomQueue(name)
}

func (c CustomQueue) Name() string       { return customQueueOpt }
func (c CustomQueue) Value() interface{} { return string(c) }
