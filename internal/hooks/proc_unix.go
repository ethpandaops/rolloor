//go:build unix

package hooks

import (
	"os/exec"
	"syscall"
	"time"
)

// isolate puts the program in its own process group so a timeout kills the
// whole tree, not just the shell, and bounds how long Run waits for pipes to
// close afterwards.
func isolate(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	cmd.WaitDelay = time.Second
}
