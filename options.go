package tasqueue

var (
	concurrencyOpt = "concurrency"
	customQueueOpt = "custom_queue"
	customMaxRetry = "custom_max_retry"
	customSchedule = "custom_schedule"

	successCallback    = "success_callback"
	processingCallback = "processing_callback"
	retryingCallback   = "retrying_callback"
	failedCallback     = "failed_callback"
)

// ConcurrencyOpt defines an option to set concurrency for workers.
type ConcurrencyOpt uint32

func Concurrency(val uint32) ConcurrencyOpt {
	return ConcurrencyOpt(val)
}

func (c ConcurrencyOpt) Name() string {
	return concurrencyOpt
}
func (c ConcurrencyOpt) Value() interface{} {
	return uint32(c)
}

// CustomQueue defines a custom queue for the task.
type CustomQueue string

func Queue(name string) Opts {
	return CustomQueue(name)
}

func (c CustomQueue) Name() string {
	return customQueueOpt
}
func (c CustomQueue) Value() interface{} {
	return string(c)
}

// CustomSchedule defines a cron schedule for the task.
type CustomSchedule string

func Schedule(spec string) Opts {
	return CustomSchedule(spec)
}

func (c CustomSchedule) Name() string {
	return customSchedule
}
func (c CustomSchedule) Value() interface{} {
	return string(c)
}

// CustomMaxRetry defines a custom value for max retries for a task.
type CustomMaxRetry uint32

func MaxRetry(val uint32) Opts {
	return CustomMaxRetry(val)
}
func (c CustomMaxRetry) Name() string {
	return customMaxRetry
}
func (c CustomMaxRetry) Value() interface{} {
	return uint32(c)
}

type SuccessCB func(*JobCtx)

func SuccessCallback(f func(*JobCtx)) Opts {
	return SuccessCB(f)
}

func (c SuccessCB) Name() string {
	return successCallback
}
func (c SuccessCB) Value() interface{} {
	return c
}

type ProcessingCB func(*JobCtx)

func ProcessingCallback(f func(*JobCtx)) Opts {
	return ProcessingCB(f)
}

func (c ProcessingCB) Name() string {
	return processingCallback
}
func (c ProcessingCB) Value() interface{} {
	return c
}

type RetryingCB func(*JobCtx)

func RetryingCallback(f func(*JobCtx)) Opts {
	return RetryingCB(f)
}

func (c RetryingCB) Name() string {
	return retryingCallback
}
func (c RetryingCB) Value() interface{} {
	return c
}

type FailedCB func(*JobCtx)

func FailedCallback(f func(*JobCtx)) Opts {
	return FailedCB(f)
}

func (c FailedCB) Name() string {
	return failedCallback
}
func (c FailedCB) Value() interface{} {
	return c
}
