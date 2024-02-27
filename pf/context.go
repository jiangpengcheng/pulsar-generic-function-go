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

package pf

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/go-logr/logr"
)

type FunctionContext struct {
	ctx             context.Context
	stub            ContextServiceClient
	logger          logr.Logger
	tenant          string
	namespace       string
	name            string
	functionId      string
	instanceId      string
	functionVersion string
	secretsProvider SecretsProvider
	inputTopics     []string
	outputTopic     string
	userConfig      map[string]string
	secretsMap      map[string]string
	messageId       *MessageId
	message         *Record
}

func NewFunctionContext(ctx context.Context, tenant, namespace, name, functionId, functionVersion, instanceId string, inputTopics []string,
	outputTopic string, userConfig map[string]string, secretsMap map[string]string, secretsProvider SecretsProvider,
	stub ContextServiceClient, logger logr.Logger) *FunctionContext {
	return &FunctionContext{
		ctx:             ctx,
		stub:            stub,
		logger:          logger,
		tenant:          tenant,
		namespace:       namespace,
		name:            name,
		functionId:      functionId,
		functionVersion: functionVersion,
		instanceId:      instanceId,
		inputTopics:     inputTopics,
		outputTopic:     outputTopic,
		userConfig:      userConfig,
		secretsProvider: secretsProvider,
		secretsMap:      secretsMap,
	}
}

func (c *FunctionContext) SetMessageId(messageId *MessageId) {
	c.messageId = messageId
}

func (c *FunctionContext) GetMessage() (*Record, error) {
	if c.messageId != nil {
		return c.message, nil
	}
	res, err := c.stub.CurrentRecord(c.ctx, c.messageId)
	if err != nil {
		return nil, err
	}
	c.message = res
	return res, nil
}

func (c *FunctionContext) GetMessageId() *MessageId {
	return c.messageId
}

func (c *FunctionContext) GetMessageKey() (string, error) {
	if c.messageId == nil {
		_, err := c.GetMessage()
		if err != nil {
			return "", err
		}
	}
	return c.message.Key, nil
}

func (c *FunctionContext) GetMessageEventTime() (int32, error) {
	if c.messageId == nil {
		_, err := c.GetMessage()
		if err != nil {
			return 0, err
		}
	}
	return c.message.EventTimestamp, nil
}

func (c *FunctionContext) GetMessageProperties() (map[string]string, error) {
	if c.messageId == nil {
		_, err := c.GetMessage()
		if err != nil {
			return nil, err
		}
	}
	properties := map[string]string{}
	err := json.Unmarshal([]byte(c.message.Properties), &properties)
	if err != nil {
		return nil, err
	}

	return properties, nil
}

func (c *FunctionContext) GetMessageTopic() (string, error) {
	if c.messageId == nil {
		_, err := c.GetMessage()
		if err != nil {
			return "", err
		}
	}
	return c.message.TopicName, nil
}

func (c *FunctionContext) GetMessagePartitionKey() (string, error) {
	if c.messageId == nil {
		_, err := c.GetMessage()
		if err != nil {
			return "", err
		}
	}
	return c.message.PartitionId, nil
}

func (c *FunctionContext) GetTenant() string {
	return c.tenant
}

func (c *FunctionContext) GetNamespace() string {
	return c.namespace
}

func (c *FunctionContext) GetFunctionName() string {
	return c.name
}

func (c *FunctionContext) GetFunctionID() string {
	return c.functionId
}

func (c *FunctionContext) GetFunctionVersion() string {
	return c.functionVersion
}

func (c *FunctionContext) GetInstanceID() string {
	return c.instanceId
}

func (c *FunctionContext) GetLogger() logr.Logger {
	return c.logger
}

func (c *FunctionContext) GetUserConfigValue(key string) string {
	if v, ok := c.userConfig[key]; ok {
		return v
	} else {
		return ""
	}
}

func (c *FunctionContext) GetUserConfigMap() map[string]string {
	return c.userConfig
}

func (c *FunctionContext) GetSecret(secretName string) (*string, error) {
	if c.secretsProvider == nil {
		return nil, errors.New("secrets provider is nil")
	}
	if v, ok := c.secretsMap[secretName]; ok {
		secret := c.secretsProvider.provideSecret(secretName, v)
		return &secret, nil
	} else {
		return nil, errors.New("secret not found")
	}
}

func (c *FunctionContext) RecordMetrics(metricName string, metricValue float64) error {
	_, err := c.stub.RecordMetrics(c.ctx, &MetricData{
		MetricName: metricName,
		Value:      metricValue,
	})
	return err
}

func (c *FunctionContext) GetInputTopics() []string {
	return c.inputTopics
}

func (c *FunctionContext) GetOutputTopic() string {
	return c.outputTopic
}

// func GetOutputSerdeClassname

func (c *FunctionContext) Publish(topic string, payload []byte) error {
	_, err := c.stub.Publish(c.ctx, &PulsarMessage{
		Topic:   topic,
		Payload: payload,
	})
	return err
}

func (c *FunctionContext) IncrCounter(key string, amount int64) error {
	_, err := c.stub.IncrCounter(c.ctx, &IncrStateKey{
		Key:    key,
		Amount: amount,
	})
	return err
}

func (c *FunctionContext) GetCounter(key string) (int64, error) {
	res, err := c.stub.GetCounter(c.ctx, &StateKey{
		Key: key,
	})
	if err != nil {
		return 0, err
	}
	return res.Value, nil
}

func (c *FunctionContext) PutState(key string, value []byte) error {
	_, err := c.stub.PutState(c.ctx, &StateKeyValue{
		Key:   key,
		Value: value,
	})
	return err
}

func (c *FunctionContext) GetState(key string) ([]byte, error) {
	res, err := c.stub.GetState(c.ctx, &StateKey{
		Key: key,
	})
	if err != nil {
		return nil, err
	}
	return res.Value, nil
}

func (c *FunctionContext) DeleteState(key string) error {
	_, err := c.stub.DeleteState(c.ctx, &StateKey{
		Key: key,
	})
	return err
}

// An unexported type to be used as the key for types in this package. This
// prevents collisions with keys defined in other packages.
type key struct{}

// contextKey is the key for FunctionContext values in context.FunctionContext. It is
// unexported; clients should use FunctionContext.NewFunctionContext and
// FunctionContext.FromContext instead of using this key directly.
var contextKey = &key{}

// NewContext returns a new FunctionContext that carries value u.
func NewContext(parent context.Context, fc *FunctionContext) context.Context {
	return context.WithValue(parent, contextKey, fc)
}

// FromContext returns the User value stored in ctx, if any.
func FromContext(ctx context.Context) (*FunctionContext, bool) {
	fc, ok := ctx.Value(contextKey).(*FunctionContext)
	return fc, ok
}
