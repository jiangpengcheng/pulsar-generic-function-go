//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

//
// This file borrows some implementations from
// {@link https://github.com/aws/aws-lambda-go/blob/master/lambda/handler.go}
//  - errorHandler
//  - validateArguments
//  - validateReturns
//  - NewFunction
//  - Process
//

package pf

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	tenant          string
	namespace       string
	name            string
	source          string
	sink            string
	instanceId      string
	functionId      string
	functionVersion string
	userConfig      string
	secretsMap      string
	logTopic        string
)

func newStderrLogger() logr.Logger {
	return funcr.New(func(prefix, args string) {
		if prefix != "" {
			_, _ = fmt.Fprintf(os.Stderr, "%s:%s\n", prefix, args)
		} else {
			_, _ = fmt.Fprintln(os.Stderr, args)
		}
	}, funcr.Options{})
}

func newPulsarLogger(ctx context.Context, stub ContextServiceClient, logTopic string) logr.Logger {
	return funcr.New(func(prefix, args string) {
		message := args
		if prefix != "" {
			message = fmt.Sprintf("%s:%s\n", prefix, args)
		}
		_, _ = stub.Publish(ctx, &PulsarMessage{
			Topic:   logTopic,
			Payload: []byte(message),
		})
	}, funcr.Options{})
}

type function interface {
	process(ctx context.Context, input []byte) ([]byte, error)
}

type pulsarFunction func(ctx context.Context, input []byte) ([]byte, error)

func (function pulsarFunction) process(ctx context.Context, input []byte) ([]byte, error) {
	output, err := function(ctx, input)
	if err != nil {
		return nil, err
	}

	return output, nil
}

func errorHandler(e error) pulsarFunction {
	return func(ctx context.Context, input []byte) ([]byte, error) {
		return nil, e
	}
}

func validateArguments(handler reflect.Type) (bool, error) {
	handlerTakesContext := false
	if handler.NumIn() > 2 {
		return false, fmt.Errorf("functions may not take more than two arguments, but function takes %d", handler.NumIn())
	} else if handler.NumIn() > 0 {
		contextType := reflect.TypeOf((*context.Context)(nil)).Elem()
		argumentType := handler.In(0)
		handlerTakesContext = argumentType.Implements(contextType)
		if handler.NumIn() > 1 && !handlerTakesContext {
			return false, fmt.Errorf("function takes two arguments, but the first is not FunctionContext. got %s", argumentType.Kind())
		}
	}

	return handlerTakesContext, nil
}

func validateReturns(handler reflect.Type) error {
	errorType := reflect.TypeOf((*error)(nil)).Elem()

	switch {
	case handler.NumOut() > 2:
		return fmt.Errorf("function may not return more than two values")
	case handler.NumOut() > 1:
		if !handler.Out(1).Implements(errorType) {
			return fmt.Errorf("function returns two values, but the second does not implement error")
		}
	case handler.NumOut() == 1:
		if !handler.Out(0).Implements(errorType) {
			return fmt.Errorf("function returns a single value, but it does not implement error")
		}
	}

	return nil
}

func newFunction(inputFunc interface{}) function {
	if inputFunc == nil {
		return errorHandler(fmt.Errorf("function is nil"))
	}
	handler := reflect.ValueOf(inputFunc)
	handlerType := reflect.TypeOf(inputFunc)
	if handlerType.Kind() != reflect.Func {
		return errorHandler(fmt.Errorf("function kind %s is not %s", handlerType.Kind(), reflect.Func))
	}

	takesContext, err := validateArguments(handlerType)
	if err != nil {
		return errorHandler(err)
	}

	if err := validateReturns(handlerType); err != nil {
		return errorHandler(err)
	}

	return pulsarFunction(func(ctx context.Context, input []byte) ([]byte, error) {
		// construct arguments
		var args []reflect.Value
		if takesContext {
			args = append(args, reflect.ValueOf(ctx))
		}

		if (handlerType.NumIn() == 1 && !takesContext) || handlerType.NumIn() == 2 {
			args = append(args, reflect.ValueOf(input))
		}
		response := handler.Call(args)

		// convert return values into ([]byte, error)
		var err error
		if len(response) > 0 {
			if errVal, ok := response[len(response)-1].Interface().(error); ok {
				err = errVal
			}
		}

		var val []byte
		if len(response) > 1 {
			val = response[0].Bytes()
		}

		return val, err
	})
}

// Start
// the entrypoint of golang function.
// Rules:
// - handler must be a function
//   - handler may take between 0 and two arguments.
//   - if there are two arguments, the first argument must satisfy the "context.FunctionContext" interface.
//   - handler may return between 0 and two arguments.
//   - if there are two return values, the second argument must be an error.
//   - if there is one return value it must be an error.
//
// Valid function signatures:
//
//	func ()
//	func () error
//	func (input) error
//	func () (output, error)
//	func (input) (output, error)
//	func (context.FunctionContext) error
//	func (context.FunctionContext, input) error
//	func (context.FunctionContext) (output, error)
//	func (context.FunctionContext, input) (output, error)
//
// Where "input" and "output" are types compatible with the "encoding/json" standard library.
// See https://golang.org/pkg/encoding/json/#Unmarshal for how deserialization behaves
func Start(funcName interface{}) {
	function := newFunction(funcName)
	defaultLogger := newStderrLogger()

	var secretsProviderImpl SecretsProvider = &EnvironmentBasedSecretsProvider{}
	ex, err := os.Executable()
	if err != nil {
		panic(err)
	}
	workDir := filepath.Dir(ex)
	channel, err := grpc.Dial("unix://"+workDir+"/context.sock", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer func(channel *grpc.ClientConn) {
		_ = channel.Close()
	}(channel)
	stub := NewContextServiceClient(channel)

	userConfigMap := make(map[string]string)
	if userConfig != "" {
		err = json.Unmarshal([]byte(userConfig), &userConfigMap)
		if err != nil {
			defaultLogger.Error(err, "Error unmarshal user configs:", err)
		}
	}

	secretsMapMap := make(map[string]string)
	if secretsMap != "" {
		err = json.Unmarshal([]byte(secretsMap), &secretsMapMap)
		if err != nil {
			defaultLogger.Error(err, "Error unmarshal secrets map:", err)
		}
	}

	var logger logr.Logger
	if logTopic != "" {
		logger = newPulsarLogger(context.Background(), stub, logTopic)
	} else {
		logger = defaultLogger
	}

	functionContext := NewFunctionContext(context.Background(), tenant, namespace, name, functionId, functionVersion, instanceId, []string{source}, sink, userConfigMap, secretsMapMap, secretsProviderImpl, stub, logger)

	reader := bufio.NewReader(os.Stdin)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// hijack the stdout so that users cannot write to it
	stdout := os.Stdout
	os.Stdout = os.Stderr

	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if err != io.EOF {
				defaultLogger.Error(err, "Error reading from stdout:", err)
			}
			break
		}
		metaLength := line[0]

		if len(line) < int(metaLength+3) {
			writeResult(stdout, []byte("error: meta length is too long"))
			continue
		}

		meta := strings.Split(string(line[1:metaLength+1]), "@")
		if len(meta) != 2 {
			writeResult(stdout, []byte("error: meta length is not 2"))
			continue
		}
		functionContext.SetMessageId(&MessageId{
			Id: meta[0],
		})

		// ignore the last `\n` byte
		msg := line[metaLength+1 : len(line)-1]
		if len(msg) == 0 {
			writeResult(stdout, []byte("error: msg length is 0"))
			continue
		}

		valuedCtx := NewContext(ctx, functionContext)
		result, err := function.process(valuedCtx, msg)
		if err != nil {
			writeResult(stdout, []byte("error: handle message: "+err.Error()))
			continue
		}

		writeResult(stdout, result)
	}
}

func writeResult(stdout *os.File, result []byte) {
	if len(result) > 0 {
		result = bytes.ReplaceAll(result, []byte("\n"), []byte(""))
		_, _ = stdout.Write(result)
	}
	_, _ = stdout.Write([]byte("\n"))
}

func init() {
	flag.StringVar(&tenant, "tenant", "", "tenant of function")
	flag.StringVar(&namespace, "namespace", "", "namespace of function")
	flag.StringVar(&name, "name", "", "name of function")
	flag.StringVar(&source, "source", "", "the source spec(in json format)")
	flag.StringVar(&sink, "sink", "", "the sink spec(in json format)")
	flag.StringVar(&instanceId, "instance_id", "", "the instance id")
	flag.StringVar(&functionId, "function_id", "", "the function id")
	flag.StringVar(&functionVersion, "function_version", "", "the function version")
	flag.StringVar(&userConfig, "user_config", "", "the user config(in json format)")
	flag.StringVar(&secretsMap, "secrets_map", "", "the secrets map(in json format)")
	flag.StringVar(&logTopic, "log_topic", "", "the log topic")
}
