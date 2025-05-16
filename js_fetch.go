package job

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/textproto"
	"strings"
	"time"

	"github.com/dop251/goja"
	"github.com/goliatone/go-errors"
)

// FetchOptions represents the options for the fetch function
type FetchOptions struct {
	Method  string            `json:"method"`
	Headers map[string]string `json:"headers"`
	Body    any               `json:"body"`
	Timeout int               `json:"timeout"` //milliseconds
}

// FetchResponse represents the response from a fetch call
type FetchResponse struct {
	Status     int                 `json:"status"`
	StatusText string              `json:"statusText"`
	Headers    map[string][]string `json:"headers"`
	URL        string              `json:"url"`
	Body       []byte              `json:"-"`
}

type jsBody struct {
	vm   *goja.Runtime
	data []byte
	used bool
}

func (b *jsBody) text() func() *goja.Promise {
	return func() *goja.Promise {
		promise, resolve, reject := b.vm.NewPromise()
		if b.used {
			reject(b.vm.NewTypeError("body has already been consumed"))
			return promise
		}
		b.used = true
		resolve(b.vm.ToValue(string(b.data)))
		return promise
	}
}

func (b *jsBody) json() func() *goja.Promise {
	return func() *goja.Promise {
		promise, resolve, reject := b.vm.NewPromise()
		if b.used {
			reject(b.vm.NewTypeError("body has already been consumed"))
			return promise
		}
		b.used = true
		var parsed any
		if err := json.Unmarshal(b.data, &parsed); err != nil {
			reject(b.vm.NewGoError(fmt.Errorf("failed to parse JSON: %w", err)))
			return promise
		}
		resolve(b.vm.ToValue(parsed))
		return promise
	}
}

func (b *jsBody) arrayBuffer() func() *goja.Promise {
	return func() *goja.Promise {
		promise, resolve, reject := b.vm.NewPromise()
		if b.used {
			reject(b.vm.NewTypeError("body has already been consumed"))
			return promise
		}
		b.used = true
		ab := b.vm.NewArrayBuffer(b.data)
		resolve(ab)
		return promise
	}
}

func (e *JSEngine) setupFetch(vm *goja.Runtime) error {
	return SetupFetch(vm)
}

func SetupFetch(vm *goja.Runtime) error {
	return vm.Set("fetch", func(call goja.FunctionCall) goja.Value {
		promise, resolve, reject := vm.NewPromise()

		if len(call.Arguments) == 0 {
			reject(vm.NewTypeError("fetch requires at least one argument"))
			return vm.ToValue(promise)
		}

		var urlStr string
		fisrArg := call.Argument(0).Export()

		var options FetchOptions = FetchOptions{
			Method:  "GET",
			Headers: make(map[string]string),
			Timeout: 30_000, // 30s
		}

		switch val := fisrArg.(type) {
		case string:
			urlStr = val
		case map[string]any:
			if rawUrl, ok := val["url"]; ok {
				if urlField, ok := rawUrl.(string); ok {
					urlStr = urlField
				} else {
					reject(vm.NewTypeError("fetch: 'url' property must be a string"))
					return vm.ToValue(promise)
				}
			} else {
				reject(vm.NewTypeError("fetch: object must have a 'url' property"))
				return vm.ToValue(promise)
			}

			if methodVal, ok := val["method"]; ok {
				if methodStr, ok := methodVal.(string); ok && methodStr != "" {
					options.Method = methodStr
				}
			}

			if headersVal, ok := val["headers"]; ok {
				if headersMap, ok := headersVal.(map[string]any); ok {
					for k, v := range headersMap {
						options.Headers[textproto.CanonicalMIMEHeaderKey(k)] = fmt.Sprint(v)
					}
				}
			}

			if bodyVal, ok := val["body"]; ok {
				options.Body = bodyVal
			}

			if timeoutVal, ok := val["timeout"]; ok {
				if t, ok := timeoutVal.(float64); ok { // float64 from JSON decode
					options.Timeout = int(t)
				}
			}
		default:
			reject(vm.NewTypeError("fetch: first argument must be a string URL or an object with at least a 'url' property"))
			return vm.ToValue(promise)
		}

		if len(call.Arguments) > 1 && !goja.IsUndefined(call.Argument(1)) {
			optsObj := call.Argument(1).ToObject(vm)
			if optsObj != nil {

				if method := optsObj.Get("method"); method != nil && !goja.IsUndefined(method) {
					options.Method = method.String()
				}

				if headers := optsObj.Get("headers"); headers != nil && !goja.IsUndefined(headers) {
					if headersObj := headers.ToObject(vm); headersObj != nil {
						for _, key := range headersObj.Keys() {
							if v := headersObj.Get(key); v != nil {
								options.Headers[textproto.CanonicalMIMEHeaderKey(key)] = v.String()
							}
						}
					}
				}

				if body := optsObj.Get("body"); body != nil && !goja.IsUndefined(body) {
					options.Body = body.Export()
				}

				if timeout := optsObj.Get("timeout"); timeout != nil && !goja.IsUndefined(timeout) {
					if t, ok := timeout.Export().(int64); ok {
						options.Timeout = int(t)
					}
				}
			}
		}

		go func() {
			resp, err := executeFetch(urlStr, options)
			if err != nil {
				reject(vm.NewGoError(err))
				return
			}
			jsResp := createJSResponse(vm, resp)
			resolve(jsResp)
		}()

		return vm.ToValue(promise)
	})
}

func executeFetch(url string, options FetchOptions) (*FetchResponse, error) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Duration(options.Timeout)*time.Millisecond,
	)
	defer cancel()

	var reqBody io.Reader
	if options.Body != nil {
		switch bodyVal := options.Body.(type) {
		case string:
			reqBody = strings.NewReader(bodyVal)
		default:
			jsonData, err := json.Marshal(options.Body)
			if err != nil {
				return nil, errors.Wrap(err, errors.CategoryInternal, "failed to marshal request body").
					WithTextCode("FETCH_MARSHAL_ERROR").
					WithMetadata(map[string]any{
						"operation": "marshal_body",
						"url":       url,
						"method":    options.Method,
						"body_type": fmt.Sprintf("%T", options.Body),
					})
			}
			reqBody = io.NopCloser(strings.NewReader(string(jsonData)))

			if _, exists := options.Headers["Content-Type"]; !exists {
				options.Headers["Content-Type"] = "application/json"
			}
		}

	}

	req, err := http.NewRequestWithContext(ctx, options.Method, url, reqBody)
	if err != nil {
		return nil, errors.Wrap(err, errors.CategoryBadInput, "failed to create request").
			WithTextCode("FETCH_REQUEST_ERROR").
			WithMetadata(map[string]any{
				"operation": "create_request",
				"url":       url,
				"method":    options.Method,
			})
	}

	for key, value := range options.Headers {
		req.Header.Add(key, value)
	}

	client := &http.Client{
		Timeout: time.Duration(options.Timeout) * time.Millisecond,
	}

	httpResp, err := client.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, errors.CategoryExternal, "request failed").
			WithTextCode("FETCH_EXECUTION_ERROR").
			WithMetadata(map[string]any{
				"operation": "execute_request",
				"url":       url,
				"method":    options.Method,
				"timeout":   options.Timeout,
			})
	}
	defer httpResp.Body.Close()

	body, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, errors.Wrap(err, errors.CategoryExternal, "failed to read response body").
			WithTextCode("FETCH_READ_BODY_ERROR").
			WithMetadata(map[string]any{
				"operation":    "read_response_body",
				"url":          url,
				"method":       options.Method,
				"status_code":  httpResp.StatusCode,
				"content_type": httpResp.Header.Get("Content-Type"),
			})
	}

	headers := make(map[string][]string)
	for k, v := range httpResp.Header {
		if len(v) > 0 {
			headers[k] = v
		}
	}

	return &FetchResponse{
		Status:     httpResp.StatusCode,
		StatusText: httpResp.Status,
		Headers:    headers,
		Body:       body,
		URL:        url,
	}, nil
}

func createJSResponse(vm *goja.Runtime, resp *FetchResponse) goja.Value {
	responseObj := vm.NewObject()

	_ = responseObj.Set("status", resp.Status)
	_ = responseObj.Set("statusText", resp.StatusText)
	_ = responseObj.Set("ok", resp.Status >= 200 && resp.Status < 300)
	_ = responseObj.Set("url", resp.URL)
	_ = responseObj.Set("headers", createHeadersObject(vm, resp.Headers))

	wrappedBody := &jsBody{
		vm:   vm,
		data: resp.Body,
		used: false,
	}

	_ = responseObj.Set("text", wrappedBody.text())
	_ = responseObj.Set("json", wrappedBody.json())
	_ = responseObj.Set("arrayBuffer", wrappedBody.arrayBuffer())

	return responseObj
}

func createHeadersObject(vm *goja.Runtime, headers map[string][]string) goja.Value {
	headersObj := vm.NewObject()

	// get(name: string): string | null
	_ = headersObj.Set("get", func(call goja.FunctionCall) goja.Value {
		name := call.Argument(0).String()
		if values, ok := headers[name]; ok && len(values) > 0 {
			return vm.ToValue(values[0])
		}
		return goja.Null()
	})

	// getAll(name: string): string[]
	_ = headersObj.Set("getAll", func(call goja.FunctionCall) goja.Value {
		name := call.Argument(0).String()
		if values, ok := headers[name]; ok {
			return vm.NewArray(values)
		}
		return vm.NewArray([]any{})
	})

	// has(name: string): boolean
	_ = headersObj.Set("has", func(call goja.FunctionCall) goja.Value {
		name := call.Argument(0).String()
		_, ok := headers[name]
		return vm.ToValue(ok)
	})

	// forEach(callback, thisArg?)
	_ = headersObj.Set("forEach", func(call goja.FunctionCall) goja.Value {
		cbArg := call.Argument(0)
		cbFn, ok := goja.AssertFunction(cbArg)
		if !ok {
			// this gets raised as an exception in JS side
			panic(vm.NewTypeError("Headers.forEach callback must be a function"))
		}

		thisArg := call.Argument(1)
		if goja.IsUndefined(thisArg) {
			thisArg = headersObj
		}

		for name, values := range headers {
			for _, val := range values {
				_, exception := cbFn(thisArg, vm.ToValue(val), vm.ToValue(name), headersObj)
				if exception != nil {
					panic(exception) // goja.Callable errs with a wrapped exceptoin
				}
			}
		}
		return goja.Undefined()
	})

	return headersObj
}
