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
	"strconv"
	"time"
)

type FunctionContext struct {
	ctx                         context.Context
	stub                        ContextServiceClient
	tenant                      string
	namespace                   string
	name                        string
	functionId                  string
	instanceId                  string
	functionVersion             string
	clusterName                 string
	secretsProvider             SecretsProvider
	inputTopics                 []string
	outputTopic                 string
	userConfig                  map[string]interface{}
	secretsMap                  map[string]string
	port                        int
	metricsPort                 int
	expectedHealthCheckInterval int32
	messageId                   *MessageId
	message                     *Record
}

func NewFunctionContext(ctx context.Context, tenant, namespace, name, functionId, functionVersion, clusterName, instanceId string, inputTopics []string,
	outputTopic string, userConfig map[string]interface{}, secretsMap map[string]string, secretsProvider SecretsProvider,
	stub ContextServiceClient, port, metricsPort, expectedHealthCheckInterval int) *FunctionContext {
	return &FunctionContext{
		ctx:                         ctx,
		stub:                        stub,
		tenant:                      tenant,
		namespace:                   namespace,
		name:                        name,
		functionId:                  functionId,
		functionVersion:             functionVersion,
		clusterName:                 clusterName,
		instanceId:                  instanceId,
		inputTopics:                 inputTopics,
		outputTopic:                 outputTopic,
		userConfig:                  userConfig,
		secretsProvider:             secretsProvider,
		secretsMap:                  secretsMap,
		port:                        port,
		metricsPort:                 metricsPort,
		expectedHealthCheckInterval: int32(expectedHealthCheckInterval),
	}
}

func (c *FunctionContext) setMessageId(messageId *MessageId) {
	c.messageId = messageId
}

// GetInstanceID returns the id of the instance that invokes the running pulsar
// function.
func (c *FunctionContext) GetInstanceID() int {
	i, _ := strconv.Atoi(c.instanceId)
	return i
}

// GetInputTopics returns a list of all input topics the pulsar function has been
// invoked on
func (c *FunctionContext) GetInputTopics() []string {
	return c.inputTopics
}

// GetOutputTopic returns the output topic the pulsar function was invoked on
func (c *FunctionContext) GetOutputTopic() string {
	return c.outputTopic
}

// GetTenantAndNamespace returns the tenant and namespace the pulsar function
// belongs to in the format of `<tenant>/<namespace>`
func (c *FunctionContext) GetTenantAndNamespace() string {
	return c.GetFuncTenant() + "/" + c.GetFuncNamespace()
}

// GetTenantAndNamespaceAndName returns the full name of the pulsar function in
// the format of `<tenant>/<namespace>/<function name>`
func (c *FunctionContext) GetTenantAndNamespaceAndName() string {
	return c.GetFuncTenant() + "/" + c.GetFuncNamespace() + "/" + c.GetFuncName()
}

// GetFuncTenant returns the tenant the pulsar function belongs to
func (c *FunctionContext) GetFuncTenant() string {
	return c.tenant
}

// GetFuncName returns the name given to the pulsar function
func (c *FunctionContext) GetFuncName() string {
	return c.name
}

// GetFuncNamespace returns the namespace the pulsar function belongs to
func (c *FunctionContext) GetFuncNamespace() string {
	return c.namespace
}

// GetFuncID returns the id of the pulsar function
func (c *FunctionContext) GetFuncID() string {
	return c.functionId
}

// GetFuncVersion returns the version of the pulsar function
func (c *FunctionContext) GetFuncVersion() string {
	return c.functionVersion
}

// GetPort returns the port the pulsar function communicates on
func (c *FunctionContext) GetPort() int {
	return c.port
}

// GetClusterName returns the name of the cluster the pulsar function is running
// in
func (c *FunctionContext) GetClusterName() string {
	return c.clusterName
}

// GetExpectedHealthCheckInterval returns the expected time between health checks
// in seconds
func (c *FunctionContext) GetExpectedHealthCheckInterval() int32 {
	return c.expectedHealthCheckInterval
}

// GetExpectedHealthCheckIntervalAsDuration returns the expected time between
// health checks in seconds as a time.Duration
func (c *FunctionContext) GetExpectedHealthCheckIntervalAsDuration() time.Duration {
	return time.Duration(c.expectedHealthCheckInterval)
}

// GetMaxIdleTime returns the amount of time the pulsar function has to respond
// to the most recent health check before it is considered to be failing.
func (c *FunctionContext) GetMaxIdleTime() int64 {
	return int64(c.GetExpectedHealthCheckIntervalAsDuration() * 3 * time.Second)
}

// GetUserConfValue returns the value of a key from the pulsar function's user
// configuration map
func (c *FunctionContext) GetUserConfValue(key string) interface{} {
	if v, ok := c.userConfig[key]; ok {
		return v
	} else {
		return ""
	}
}

// GetUserConfMap returns the pulsar function's user configuration map
func (c *FunctionContext) GetUserConfMap() map[string]interface{} {
	return c.userConfig
}

// GetSecret gets the secret value of the given secretName
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

// Publish publishes payload to the given topic
func (c *FunctionContext) Publish(topic string, payload []byte) (*SendMessageId, error) {
	messageId, err := c.stub.Publish(c.ctx, &PulsarMessage{
		Topic:   topic,
		Payload: payload,
	})
	return messageId, err
}

// GetCurrentRecord gets the current message from the function context
func (c *FunctionContext) GetCurrentRecord() (*Record, error) {
	if c.message != nil {
		return c.message, nil
	}
	res, err := c.stub.CurrentRecord(c.ctx, c.messageId)
	if err != nil {
		return nil, err
	}
	c.message = res
	return res, nil
}

// GetMessageId gets the current message id
func (c *FunctionContext) GetMessageId() *MessageId {
	return c.messageId
}

// GetMessageKey gets the current message key
func (c *FunctionContext) GetMessageKey() (string, error) {
	if c.message == nil {
		_, err := c.GetCurrentRecord()
		if err != nil {
			return "", err
		}
	}
	return c.message.Key, nil
}

// GetMessageEventTime gets the current message's event time
func (c *FunctionContext) GetMessageEventTime() (int32, error) {
	if c.message == nil {
		_, err := c.GetCurrentRecord()
		if err != nil {
			return 0, err
		}
	}
	return c.message.EventTimestamp, nil
}

// GetMessageProperties gets the current message's properties
func (c *FunctionContext) GetMessageProperties() (map[string]string, error) {
	if c.message == nil {
		_, err := c.GetCurrentRecord()
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

// GetMessageTopic gets the current message topic
func (c *FunctionContext) GetMessageTopic() (string, error) {
	if c.message == nil {
		_, err := c.GetCurrentRecord()
		if err != nil {
			return "", err
		}
	}
	return c.message.TopicName, nil
}

// GetMessagePartitionKey gets the current message's partition key
func (c *FunctionContext) GetMessagePartitionKey() (string, error) {
	if c.message == nil {
		_, err := c.GetCurrentRecord()
		if err != nil {
			return "", err
		}
	}
	return c.message.PartitionId, nil
}

// GetMetricsPort returns the port the pulsar function metrics listen on
func (c *FunctionContext) GetMetricsPort() int {
	return c.metricsPort
}

// RecordMetric records an observation to the user_metric summary with the provided value
func (c *FunctionContext) RecordMetric(metricName string, metricValue float64) error {
	_, err := c.stub.RecordMetrics(c.ctx, &MetricData{
		MetricName: metricName,
		Value:      metricValue,
	})
	return err
}

// IncrCounter incr a counter for the given key in state store
func (c *FunctionContext) IncrCounter(key string, amount int64) error {
	_, err := c.stub.IncrCounter(c.ctx, &IncrStateKey{
		Key:    key,
		Amount: amount,
	})
	return err
}

// GetCounter gets the counter value of the key from state store
func (c *FunctionContext) GetCounter(key string) (int64, error) {
	res, err := c.stub.GetCounter(c.ctx, &StateKey{
		Key: key,
	})
	if err != nil {
		return 0, err
	}
	return res.Value, nil
}

// PutState puts value for the key to state store
func (c *FunctionContext) PutState(key string, value []byte) error {
	_, err := c.stub.PutState(c.ctx, &StateKeyValue{
		Key:   key,
		Value: value,
	})
	return err
}

// GetState gets the value of the key from state store
func (c *FunctionContext) GetState(key string) ([]byte, error) {
	res, err := c.stub.GetState(c.ctx, &StateKey{
		Key: key,
	})
	if err != nil {
		return nil, err
	}
	return res.Value, nil
}

// DeleteState deletes the key from state store
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
