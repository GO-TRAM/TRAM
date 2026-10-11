// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package controller_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	tramv1 "codeberg.org/eljoth/tram/internal/adapters/k8scrd/api/v1"
	"codeberg.org/eljoth/tram/internal/adapters/k8scrd/internal/controller"
	"codeberg.org/eljoth/tram/internal/core/domain"
)

type fakeCore struct {
	streams map[string]domain.StreamSpec
	upserts int
}

func (c *fakeCore) UpsertStream(_ context.Context, s domain.StreamSpec) (*domain.StreamSpec, error) {
	c.upserts++
	c.streams[s.Name] = s
	return &s, nil
}

func (c *fakeCore) DeleteStream(_ context.Context, name string) error {
	if _, ok := c.streams[name]; !ok {
		return domain.ErrStreamNotFound
	}
	delete(c.streams, name)
	return nil
}

func (c *fakeCore) GetStream(_ context.Context, name string) (*domain.StreamSpec, error) {
	s, ok := c.streams[name]
	if !ok {
		return nil, domain.ErrStreamNotFound
	}
	return &s, nil
}

func (c *fakeCore) GetStreams(context.Context) ([]domain.StreamSpec, error) { return nil, nil }

func (c *fakeCore) Watch(context.Context, chan domain.StreamSpecWatchEvent) error { return nil }

func newStream(namespace string) *tramv1.Stream {
	return &tramv1.Stream{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "s"},
		Spec: tramv1.StreamSpec{
			Source: tramv1.Spec{
				Name: "in",
				Type: "otlp",
				Spec: &runtime.RawExtension{Raw: []byte(`{"http_port":4318}`)},
			},
			Sinks: []tramv1.ReceiverSpec{{
				Name: "out",
				Type: "nats",
				From: "in",
				Spec: &runtime.RawExtension{Raw: []byte(`{"subject":{"register":["a.>"]}}`)},
			}},
		},
	}
}

func newReconciler(t *testing.T, objs ...*tramv1.Stream) (*controller.StreamReconciler, *fakeCore) {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, tramv1.AddToScheme(scheme))

	builder := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&tramv1.Stream{})
	for _, obj := range objs {
		builder = builder.WithObjects(obj)
	}

	core := &fakeCore{streams: map[string]domain.StreamSpec{}}
	return &controller.StreamReconciler{Client: builder.Build(), Scheme: scheme, Core: core}, core
}

func request(namespace string) ctrl.Request {
	return ctrl.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: "s"}}
}

func TestReconcileUpsertsOnlyOnChange(t *testing.T) {
	t.Parallel()

	r, core := newReconciler(t, newStream("default"))

	for range 3 {
		_, err := r.Reconcile(t.Context(), request("default"))
		require.NoError(t, err)
	}
	assert.Equal(t, 1, core.upserts)

	var stream tramv1.Stream
	require.NoError(t, r.Get(t.Context(), types.NamespacedName{Namespace: "default", Name: "s"}, &stream))
	assert.True(t, stream.Status.Active)

	stream.Spec.Source.Spec = &runtime.RawExtension{Raw: []byte(`{"http_port":4319}`)}
	require.NoError(t, r.Update(t.Context(), &stream))
	_, err := r.Reconcile(t.Context(), request("default"))
	require.NoError(t, err)
	assert.Equal(t, 2, core.upserts)
}

func TestReconcileKeepsNamespacesApart(t *testing.T) {
	t.Parallel()

	r, core := newReconciler(t, newStream("a"), newStream("b"))

	for _, ns := range []string{"a", "b"} {
		_, err := r.Reconcile(t.Context(), request(ns))
		require.NoError(t, err)
	}
	require.Len(t, core.streams, 2)

	var stream tramv1.Stream
	require.NoError(t, r.Get(t.Context(), types.NamespacedName{Namespace: "a", Name: "s"}, &stream))
	require.NoError(t, r.Delete(t.Context(), &stream))
	_, err := r.Reconcile(t.Context(), request("a"))
	require.NoError(t, err)

	assert.NotContains(t, core.streams, "a.s")
	assert.Contains(t, core.streams, "b.s")

	_, err = r.Reconcile(t.Context(), request("a"))
	assert.NoError(t, err)
}

func TestToDomainKeepsSpecTypes(t *testing.T) {
	t.Parallel()

	spec, err := controller.ToDomain("default.s", *newStream("default"))
	require.NoError(t, err)

	assert.Equal(t, "default.s", spec.Name)
	assert.Equal(t, map[string]any{"http_port": float64(4318)}, spec.Source.Spec)
	assert.Equal(t, map[string]any{"subject": map[string]any{"register": []any{"a.>"}}}, spec.Sinks[0].Spec)
}
