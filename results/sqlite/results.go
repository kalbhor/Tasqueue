package sqlite

import (
	"context"
	"fmt"
)

type Results struct{}

func (results *Results) Get(ctx context.Context, id string) ([]byte, error) {
	return nil, fmt.Errorf("not implemented")
}

func (results *Results) NilError() error {
	return fmt.Errorf("not implemented")
}

func (results *Results) Set(ctx context.Context, id string, b []byte) error {
	return fmt.Errorf("not implemented")
}

func (results *Results) DeleteJob(ctx context.Context, id string) error {
	return fmt.Errorf("not implemented")
}

func (results *Results) GetFailed(ctx context.Context) ([]string, error) {
	return nil, fmt.Errorf("not implemented")
}

func (results *Results) GetSuccess(ctx context.Context) ([]string, error) {
	return nil, fmt.Errorf("not implemented")
}

func (results *Results) SetFailed(ctx context.Context, id string) error {
	return fmt.Errorf("not implemented")
}

func (results *Results) SetSuccess(ctx context.Context, id string) error {
	return fmt.Errorf("not implemented")
}
