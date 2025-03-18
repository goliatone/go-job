package js

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/dop251/goja"
	"github.com/goliatone/go-command"
)

// FetchOptions represents the options for the fetch function
type FetchOptions struct {
	Method  string            `json:"string"`
	Headers map[string]string `json:"headers"`
	Body    any               `json:"body"`
	Timeout int               `json:"timeout"` //milliseconds
}

// FetchResponse represents the response from a fetch call
type FetchResponse struct {
	Status     int               `json:"status"`
	StatusText string            `json:"statusText"`
	Headers    map[string]string `json:"headers"`
	URL        string            `json:"url"`
	Body       []byte            `json:"-"`
}

func (e *Engine) setupFetch(vm *goja.Runtime) error {
	return vm.Set("fetch", func(call goja.FunctionCall) goja.Value {
		promise, resolve, reject := vm.NewPromise()

		urlArg := call.Argument(0)
		if urlArg == nil || goja.IsUndefined(urlArg) {
			reject(vm.NewTypeError("URL is required"))
			return vm.ToValue(promise)
		}

		url := urlArg.String()
		var options FetchOptions = FetchOptions{
			Method:  "GET",
			Headers: make(map[string]string),
			Timeout: 30_000, // 30s
		}

		if len(call.Arguments) > 1 && !goja.IsUndefined(call.Argument(1)) {
			optsObj := call.Argument(1).ToObject(vm)
			if optsObj != nil {

				// method
				if method := optsObj.Get("method"); method != nil && !goja.IsUndefined(method) {
					options.Method = method.String()
				}

				// headers
				if headers := optsObj.Get("headers"); headers != nil && !goja.IsUndefined(headers) {
					if headersObj := headers.ToObject(vm); headersObj != nil {
						for _, key := range headersObj.Keys() {
							if v := headersObj.Get(key); v != nil {
								options.Headers[key] = v.String()
							}
						}
					}
				}

				// body
				if body := optsObj.Get("body"); body != nil && !goja.IsUndefined(body) {
					options.Body = body.Export()
				}

				// timeout
				if timeout := optsObj.Get("timeout"); timeout != nil && !goja.IsUndefined(timeout) {
					if t, ok := timeout.Export().(int64); ok {
						options.Timeout = int(t)
					}
				}
			}
		}
		go func() {
			resp, err := e.executeFetch(url, options)
			if err != nil {
				reject(vm.NewGoError(err))
				return
			}
			jsResp := e.createJSResponse(vm, resp)
			resolve(jsResp)
		}()

		return vm.ToValue(promise)
	})
}

func (e *Engine) executeFetch(url string, options FetchOptions) (*FetchResponse, error) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Duration(options.Timeout)*time.Microsecond,
	)
	defer cancel()

	var reqBody io.Reader
	if options.Body != nil {
		jsonData, err := json.Marshal(options.Body)
		if err != nil {
			return nil, command.WrapError("FetchError", "failed to marshal request body", err)
		}
		reqBody = io.NopCloser(strings.NewReader(string(jsonData)))

		// TODO: use comparison that is case insensitive
		if _, exists := options.Headers["Content-Type"]; !exists {
			options.Headers["Content-Type"] = "application/json"
		}
	}

	req, err := http.NewRequestWithContext(ctx, options.Method, url, reqBody)
	if err != nil {
		return nil, command.WrapError("FetchError", "failed to create request", err)
	}

	for key, value := range options.Headers {
		req.Header.Add(key, value)
	}

	client := &http.Client{
		Timeout: time.Duration(options.Timeout) * time.Millisecond,
	}

	httpResp, err := client.Do(req)
	if err != nil {
		return nil, command.WrapError("FetchError", "request failed", err)
	}
	defer httpResp.Body.Close()

	body, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, command.WrapError("FetchError", "failed to read response body", err)
	}

	headers := make(map[string]string)
	for k, v := range httpResp.Header {
		if len(v) > 0 {
			headers[k] = v[0]
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

func (e *Engine) createJSResponse(vm *goja.Runtime, resp *FetchResponse) goja.Value {
	responseObj := vm.NewObject()

	_ = responseObj.Set("status", resp.Status)
	_ = responseObj.Set("statusText", resp.StatusText)
	_ = responseObj.Set("ok", resp.Status >= 200 && resp.Status < 300)
	_ = responseObj.Set("url", resp.URL)

	headers := vm.NewObject()
	for k, v := range resp.Headers {
		_ = headers.Set(k, v)
	}
	_ = responseObj.Set("headers", headers)

	_ = responseObj.Set("text", func() *goja.Promise {
		promise, resolve, _ := vm.NewPromise()
		resolve(vm.ToValue(string(resp.Body)))
		return promise
	})

	_ = responseObj.Set("json", func() *goja.Promise {
		promise, resolve, reject := vm.NewPromise()

		var jsonData any
		if err := json.Unmarshal(resp.Body, &jsonData); err != nil {
			reject(vm.NewGoError(fmt.Errorf("failed to parse JSON: %w", err)))
			return promise
		}
		resolve(vm.ToValue(jsonData))
		return promise
	})

	_ = responseObj.Set("arrayBuffer", func() *goja.Promise {
		promise, resolve, _ := vm.NewPromise()
		arrayBuffer := vm.NewArrayBuffer(resp.Body)
		resolve(arrayBuffer)
		return promise
	})

	return responseObj
}
