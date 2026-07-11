//go:build windows

package main

import (
	"context"
	"os/exec"
	"time"
)

func interruptibleCommand(ctx context.Context, bin string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.WaitDelay = 5 * time.Second
	return cmd
}
