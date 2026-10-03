//go:build !darwin && !linux

package drivesync

import "context"

func watchDirectory(ctx context.Context, dir string, wake chan<- struct{}) (func(), string, error) {
	return func() {}, "polling", nil
}
