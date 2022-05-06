package tasks

import (
	"encoding/json"
	"fmt"
)

type SumPayload struct {
	Arg1 int `json:"arg1"`
	Arg2 int `json:"arg2"`
}

// SumProcessor prints the sum of two integer arguements.
func SumProcessor(b []byte) error {
	var pl SumPayload
	if err := json.Unmarshal(b, &pl); err != nil {
		return err
	}

	fmt.Println(pl.Arg1 + pl.Arg2)

	return nil
}
