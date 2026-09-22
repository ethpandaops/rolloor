//go:build !unix

package hooks

import (
	"os/exec"
	"time"
)

func isolate(cmd *exec.Cmd) {
	cmd.WaitDelay = time.Second
}
