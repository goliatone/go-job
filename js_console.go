package job

import (
	"github.com/dop251/goja"
	"github.com/dop251/goja_nodejs/console"
)

func (e *JSEngine) setupConsole(vm *goja.Runtime) error {
	console.Enable(vm)
	return nil
}
