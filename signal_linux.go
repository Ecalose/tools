//go:build linux

package tools

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

func Signal(preCtx context.Context, fun func()) {
	if fun == nil || preCtx == nil {
		return
	}
	ch := make(chan os.Signal, 1)
	signal.Notify(ch,
		syscall.SIGHUP, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGILL, syscall.SIGTRAP,
		syscall.SIGABRT, syscall.SIGBUS, syscall.SIGFPE, syscall.SIGSEGV, syscall.SIGPIPE,
		syscall.SIGALRM, syscall.SIGTERM)
	select {
	case <-preCtx.Done():
		fun()
		signal.Stop(ch)
	case s := <-ch:
		fun()
		signal.Stop(ch)
		signal.Reset(s)
		if p, err := os.FindProcess(os.Getpid()); err == nil && p != nil {
			sg, ok := s.(syscall.Signal)
			if ok && sg == syscall.SIGINT {
				p.Signal(syscall.SIGKILL)
			} else {
				p.Signal(s)
			}
		}
	}
}
