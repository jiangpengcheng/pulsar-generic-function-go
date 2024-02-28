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

	"github.com/sirupsen/logrus"
)

type PulsarHook struct {
	ctx      context.Context
	stub     ContextServiceClient
	logTopic string
}

func (hook *PulsarHook) Fire(entry *logrus.Entry) error {
	message, err := entry.String()
	if err != nil {
		return err
	}
	_, err = hook.stub.Publish(hook.ctx, &PulsarMessage{
		Topic:   hook.logTopic,
		Payload: []byte(message),
	})
	if err != nil {
		return err
	}
	return nil
}

func (hook *PulsarHook) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.InfoLevel,
		logrus.WarnLevel,
		logrus.ErrorLevel,
		logrus.FatalLevel,
		logrus.PanicLevel,
	}
}
