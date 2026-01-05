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

	decoder, err := newDecoder(target.Interface())
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

func decodeParamsByType(msgType reflect.Type, params map[string]any) (reflect.Value, error) {
	if msgType == nil {
		return reflect.Value{}, fmt.Errorf("message type not configured")
	}
	if params == nil {
		return reflect.Zero(msgType), nil
	}

	var target reflect.Value
	if msgType.Kind() == reflect.Ptr {
		target = reflect.New(msgType.Elem())
	} else {
		target = reflect.New(msgType)
	}

	decoder, err := newDecoder(target.Interface())
	if err != nil {
		return reflect.Value{}, err
	}
	if err := decoder.Decode(params); err != nil {
		return reflect.Value{}, err
	}

	if msgType.Kind() == reflect.Ptr {
		return target, nil
	}
	return target.Elem(), nil
}

func newValue(msgType reflect.Type) reflect.Value {
	if msgType.Kind() == reflect.Ptr {
		return reflect.New(msgType.Elem())
	}
	return reflect.New(msgType)
}

func newDecoder(result any) (*mapstructure.Decoder, error) {
	return mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		Result:           result,
		TagName:          "json",
		WeaklyTypedInput: true,
	})
}
