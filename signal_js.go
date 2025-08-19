//go:build js

package tools

import (
	"context"
)

func Signal(preCtx context.Context, fun func()) {
	if fun == nil || preCtx == nil {
		return
	}
	<-preCtx.Done()
	fun()
}
