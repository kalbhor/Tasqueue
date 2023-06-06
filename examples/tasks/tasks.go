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
