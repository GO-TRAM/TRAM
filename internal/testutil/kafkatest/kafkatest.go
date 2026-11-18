// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package kafkatest starts a Kafka broker in a container for tests.
package kafkatest

import (
	"context"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"codeberg.org/eljoth/tram/internal/testutil/containertest"
)

// Image is the only image the testcontainers module supports, Kafka in KRaft
// mode.
const Image = "confluentinc/confluent-local:7.5.0"

// Start starts Kafka and returns the addresses of its brokers. The test is
// skipped without a container runtime.
func Start(tb testing.TB) []string {
	tb.Helper()
	ctx := context.Background()
	containertest.SkipWithoutRuntime(ctx, tb)

	container, err := tckafka.Run(ctx, Image, tckafka.WithClusterID("tram-test"))
	testcontainers.CleanupContainer(tb, container)
	if err != nil {
		tb.Fatalf("start Kafka container: %v", err)
	}
	brokers, err := container.Brokers(ctx)
	if err != nil {
		tb.Fatalf("get Kafka brokers: %v", err)
	}
	return brokers
}

// CreateTopic creates topic with partitions, as a deployment does outside
// TRAM.
func CreateTopic(tb testing.TB, brokers []string, topic string, partitions int32) {
	tb.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		tb.Fatalf("connect to Kafka: %v", err)
	}
	defer cl.Close()
	resp, err := kadm.NewClient(cl).CreateTopic(context.Background(), partitions, 1, nil, topic)
	if err == nil {
		err = resp.Err
	}
	if err != nil {
		tb.Fatalf("create topic %s: %v", topic, err)
	}
}
