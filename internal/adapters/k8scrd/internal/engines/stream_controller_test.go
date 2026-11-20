// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package engines_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	tramv1 "github.com/go-tram/tram/internal/adapters/k8scrd/api/v1"
	tramv1alpha1 "github.com/go-tram/tram/internal/adapters/k8scrd/api/v1alpha1"
	"github.com/go-tram/tram/internal/adapters/k8scrd/internal/engines"
)

const mapperLabel = "engine.tram.io/mapper"

const namespace = "telemetry"

func streamKey() types.NamespacedName {
	return types.NamespacedName{Namespace: namespace, Name: "resource-to-router-audit"}
}

func newStreamReconciler(t *testing.T, objs ...client.Object) (*engines.StreamReconciler, client.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, tramv1.AddToScheme(scheme))
	require.NoError(t, tramv1alpha1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).
		WithStatusSubresource(&tramv1.Stream{}).Build()
	return &engines.StreamReconciler{Client: c}, c
}

func stream(labels map[string]string, engineNames ...string) *tramv1.Stream {
	s := &tramv1.Stream{}
	s.Namespace, s.Name, s.Labels, s.Generation = streamKey().Namespace, streamKey().Name, labels, 3
	for _, engine := range engineNames {
		s.Spec.EngineRefs = append(s.Spec.EngineRefs, tramv1.EngineReference{Name: engine})
	}
	return s
}

func engine(name string) *tramv1alpha1.Engine {
	e := &tramv1alpha1.Engine{}
	e.Namespace, e.Name = namespace, name
	return e
}

func reconcileStream(t *testing.T, r *engines.StreamReconciler) {
	t.Helper()
	_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: streamKey()})
	require.NoError(t, err)
}

func getStream(t *testing.T, c client.Client) *tramv1.Stream {
	t.Helper()
	var s tramv1.Stream
	require.NoError(t, c.Get(t.Context(), streamKey(), &s))
	return &s
}

func TestStreamReconcileDerivesEngineLabels(t *testing.T) {
	t.Parallel()

	r, c := newStreamReconciler(t, stream(
		map[string]string{"team": "telemetry", "engine.tram.io/old": "", mapperLabel: "yes"},
		"mapper", "mapper-v2",
	))
	reconcileStream(t, r)

	assert.Equal(t, map[string]string{
		"team":                     "telemetry",
		mapperLabel:                "",
		"engine.tram.io/mapper-v2": "",
	}, getStream(t, c).Labels)
}

func TestStreamReconcileRemovesEngineLabelsWithoutReferences(t *testing.T) {
	t.Parallel()

	r, c := newStreamReconciler(t, stream(map[string]string{mapperLabel: "", "team": "telemetry"}))
	reconcileStream(t, r)

	s := getStream(t, c)
	assert.Equal(t, map[string]string{"team": "telemetry"}, s.Labels)
	assert.Empty(t, s.Status.Engines)
}

func TestStreamReconcileLeavesMatchingStreamAlone(t *testing.T) {
	t.Parallel()

	r, c := newStreamReconciler(
		t,
		stream(map[string]string{mapperLabel: ""}, "mapper"),
		engine("mapper"),
	)
	reconcileStream(t, r)
	before := getStream(t, c).ResourceVersion
	reconcileStream(t, r)

	assert.Equal(t, before, getStream(t, c).ResourceVersion, "a Stream whose labels and status match is not patched")
}

func TestStreamReconcileReportsWhetherEnginesExist(t *testing.T) {
	t.Parallel()

	r, c := newStreamReconciler(t, stream(nil, "mapper", "missing"), engine("mapper"))
	reconcileStream(t, r)

	s := getStream(t, c)
	require.Len(t, s.Status.Engines, 2)

	assert.Equal(t, "mapper", s.Status.Engines[0].Name)
	found := meta.FindStatusCondition(s.Status.Engines[0].Conditions, tramv1.ConditionAccepted)
	require.NotNil(t, found)
	assert.Equal(t, metav1.ConditionTrue, found.Status)
	assert.Equal(t, "EngineFound", found.Reason)
	assert.Equal(t, int64(3), found.ObservedGeneration)

	assert.Equal(t, "missing", s.Status.Engines[1].Name)
	missing := meta.FindStatusCondition(s.Status.Engines[1].Conditions, tramv1.ConditionAccepted)
	require.NotNil(t, missing)
	assert.Equal(t, metav1.ConditionFalse, missing.Status)
	assert.Equal(t, "EngineNotFound", missing.Reason)

	// Once the Engine exists, the Stream is accepted, keeping the first
	// entry's transition time.
	require.NoError(t, c.Create(t.Context(), engine("missing")))
	reconcileStream(t, r)
	s2 := getStream(t, c)
	assert.Equal(t, metav1.ConditionTrue,
		meta.FindStatusCondition(s2.Status.Engines[1].Conditions, tramv1.ConditionAccepted).Status)
	assert.Equal(t, found.LastTransitionTime.Unix(),
		meta.FindStatusCondition(s2.Status.Engines[0].Conditions, tramv1.ConditionAccepted).LastTransitionTime.Unix())
}

func TestStreamReconcileDropsStatusOfRemovedReferences(t *testing.T) {
	t.Parallel()

	r, c := newStreamReconciler(t, stream(nil, "mapper", "routers"), engine("mapper"))
	reconcileStream(t, r)

	s := getStream(t, c)
	base := s.DeepCopy()
	s.Spec.EngineRefs = s.Spec.EngineRefs[:1]
	require.NoError(t, c.Patch(t.Context(), s, client.MergeFrom(base)))
	reconcileStream(t, r)

	engineStatus := getStream(t, c).Status.Engines
	require.Len(t, engineStatus, 1)
	assert.Equal(t, "mapper", engineStatus[0].Name)
}

func TestStreamReconcileIgnoresMissingStream(t *testing.T) {
	t.Parallel()

	r, _ := newStreamReconciler(t)
	reconcileStream(t, r)
}
