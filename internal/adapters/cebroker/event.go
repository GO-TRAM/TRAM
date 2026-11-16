// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package cebroker carries Tram events as CloudEvents over a message broker:
// the event format shared by the cloudevent sink and source, and a driver per
// broker built on the protocol bindings of the CloudEvents SDK.
package cebroker

import (
	"fmt"
	"strings"
	"time"

	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/google/uuid"

	"codeberg.org/eljoth/tram/internal/core/domain"
)

const (
	// ContentType of the data: one serialized OTLP LogsData, MetricsData or
	// TracesData.
	ContentType = "application/x-protobuf"

	// typePrefix is followed by the signal, e.g. io.opentelemetry.logs.
	typePrefix = "io.opentelemetry."
)

// TypeOf returns the CloudEvents type of a signal's data.
func TypeOf(signal domain.Signal) string {
	return typePrefix + string(signal)
}

// SignalOf returns the signal of a CloudEvents type written by TypeOf.
func SignalOf(eventType string) (domain.Signal, error) {
	signal := domain.Signal(strings.TrimPrefix(eventType, typePrefix))
	switch {
	case !strings.HasPrefix(eventType, typePrefix):
	case signal == domain.SignalLogs, signal == domain.SignalMetrics, signal == domain.SignalTraces:
		return signal, nil
	}
	return "", fmt.Errorf("unknown CloudEvents type %q, expected %s<logs|metrics|traces>", eventType, typePrefix)
}

// NewEvent wraps the OTLP data of one signal. source identifies the sink,
// subject is the destination the event is published to.
func NewEvent(source, subject string, signal domain.Signal, data []byte) *event.Event {
	e := event.New()
	e.SetID(uuid.NewString())
	e.SetSource(source)
	e.SetType(TypeOf(signal))
	e.SetSubject(subject)
	e.SetTime(time.Now())
	// []byte is stored as it is, without encoding it again.
	_ = e.SetData(ContentType, data)
	return &e
}

// EventOf returns the Tram event that e carries.
func EventOf(e *event.Event) (*domain.Event, error) {
	signal, err := SignalOf(e.Type())
	if err != nil {
		return nil, err
	}
	if contentType := e.DataContentType(); contentType != ContentType {
		return nil, fmt.Errorf("unsupported data content type %q, expected %s", contentType, ContentType)
	}
	return domain.UnmarshalOTLPData(signal, e.Data())
}
