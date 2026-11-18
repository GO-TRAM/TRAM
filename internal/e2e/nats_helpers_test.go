// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package e2e_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"

	"codeberg.org/eljoth/tram/internal/testutil/natstest"
)

// natsServer is a NATS server with JetStream for the broker sink and source.
// TRAM creates no streams, so the tests set them up like a deployment does
// outside TRAM.
type natsServer struct {
	url     string
	js      jetstream.JetStream
	streams atomic.Int64
}

func startNATS(t *testing.T) *natsServer {
	t.Helper()
	url := natstest.Start(t)
	nc, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	return &natsServer{url: url, js: js}
}

// stream returns a name unique to the server, without creating the stream.
func (n *natsServer) stream(name string) string {
	return fmt.Sprintf("e2e%d-%s", n.streams.Add(1), name)
}

// read returns the messages of a stream, none if it does not exist.
func (n *natsServer) read(t *testing.T, name string) []*jetstream.RawStreamMsg {
	t.Helper()
	ctx := context.Background()
	stream, err := n.js.Stream(ctx, name)
	if err != nil {
		return nil
	}
	info, err := stream.Info(ctx)
	require.NoError(t, err)

	var messages []*jetstream.RawStreamMsg
	for seq := info.State.FirstSeq; seq <= info.State.LastSeq && info.State.Msgs > 0; seq++ {
		m, getErr := stream.GetMsg(ctx, seq)
		require.NoError(t, getErr)
		messages = append(messages, m)
	}
	return messages
}
