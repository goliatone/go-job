package command

import (
	"fmt"
	"reflect"

	"github.com/go-viper/mapstructure/v2"
)

func decodeParams[T any](params map[string]any) (T, error) {
	var zero T
	if params == nil {
		return zero, nil
	}

	msgType := reflect.TypeOf((*T)(nil)).Elem()
	target := newValue(msgType)

	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		Result:           target.Interface(),
		TagName:          "json",
		WeaklyTypedInput: true,
	})
	if err != nil {
		return zero, err
	}
	if err := decoder.Decode(params); err != nil {
		return zero, err
	}

	if msgType.Kind() == reflect.Ptr {
		value, ok := target.Interface().(T)
		if !ok {
			return zero, fmt.Errorf("decoded message type mismatch")
		}
		return value, nil
	}

	value, ok := target.Elem().Interface().(T)
	if !ok {
		return zero, fmt.Errorf("decoded message type mismatch")
	}
	return value, nil
}

func newValue(msgType reflect.Type) reflect.Value {
	if msgType.Kind() == reflect.Ptr {
		return reflect.New(msgType.Elem())
	}
	return reflect.New(msgType)
}
