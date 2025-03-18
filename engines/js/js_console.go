package js

import (
	"fmt"

	"github.com/dop251/goja"
)

func (e *Engine) setupConsole(vm *goja.Runtime) error {
	console := map[string]any{
		"log": func(args ...any) {
			fmt.Printf("[JS] [INFO] %v\n", args)
		},
		"error": func(args ...any) {
			fmt.Printf("[JS] [ERROR] %v\n", args)
		},
	}
	return vm.Set("console", console)
}
