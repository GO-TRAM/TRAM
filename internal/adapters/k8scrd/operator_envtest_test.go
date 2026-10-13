// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package k8scrd_test

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/yaml"

	"codeberg.org/eljoth/tram/internal/adapters/k8scrd"
	tramv1 "codeberg.org/eljoth/tram/internal/adapters/k8scrd/api/v1"
	"codeberg.org/eljoth/tram/internal/core/domain"
)

const (
	eventuallyTimeout = 15 * time.Second
	pollInterval      = 50 * time.Millisecond
	settleTime        = time.Second
)

type cluster struct {
	env   *envtest.Environment
	admin client.Client
}

func startCluster(t *testing.T) *cluster {
	t.Helper()
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is not set; run `task test:envtest`")
	}

	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := env.Start()
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, env.Stop()) })

	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, tramv1.AddToScheme(scheme))
	admin, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)

	return &cluster{env: env, admin: admin}
}

func (c *cluster) kubeconfig(t *testing.T, user envtest.User) string {
	t.Helper()
	authUser, err := c.env.AddUser(user, nil)
	require.NoError(t, err)
	content, err := authUser.KubeConfig()
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "kubeconfig")
	require.NoError(t, os.WriteFile(path, content, 0o600))
	return path
}

func (c *cluster) adminKubeconfig(t *testing.T) string {
	t.Helper()
	return c.kubeconfig(t, envtest.User{Name: "admin", Groups: []string{"system:masters"}})
}

func (c *cluster) newNamespace(t *testing.T) string {
	t.Helper()
	ns := &corev1.Namespace{GenerateName: "tram-test-"}
	require.NoError(t, c.admin.Create(t.Context(), ns))
	return ns.Name
}

func (c *cluster) createStream(t *testing.T, namespace, name string, labels map[string]string) {
	t.Helper()
	stream := &tramv1.Stream{
		Namespace: namespace, Name: name, Labels: labels,
		Spec: tramv1.StreamSpec{
			Source: tramv1.Spec{
				Name: "in",
				Type: "otlp",
				Spec: &runtime.RawExtension{Raw: []byte(`{"http_port":4318}`)},
			},
			Sinks: []tramv1.ReceiverSpec{{Name: "out", Type: "debug", From: "in"}},
		},
	}
	require.NoError(t, c.admin.Create(t.Context(), stream))
}

func (c *cluster) setLabels(t *testing.T, namespace, name string, labels map[string]string) {
	t.Helper()
	var stream tramv1.Stream
	require.NoError(t, c.admin.Get(t.Context(), types.NamespacedName{Namespace: namespace, Name: name}, &stream))
	base := stream.DeepCopy()
	stream.Labels = labels
	require.NoError(t, c.admin.Patch(t.Context(), &stream, client.MergeFrom(base)))
}

func (c *cluster) requireActive(t *testing.T, namespace, name string) {
	t.Helper()
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		var stream tramv1.Stream
		key := types.NamespacedName{Namespace: namespace, Name: name}
		assert.NoError(collect, c.admin.Get(t.Context(), key, &stream))
		assert.True(collect, stream.Status.Active)
	}, eventuallyTimeout, pollInterval)
}

type syncCore struct {
	mu      sync.Mutex
	streams map[string]domain.StreamSpec
}

func newSyncCore() *syncCore {
	return &syncCore{streams: map[string]domain.StreamSpec{}}
}

func (c *syncCore) UpsertStream(_ context.Context, s domain.StreamSpec) (*domain.StreamSpec, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.streams[s.Name] = s
	return &s, nil
}

func (c *syncCore) DeleteStream(_ context.Context, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.streams[name]; !ok {
		return domain.ErrStreamNotFound
	}
	delete(c.streams, name)
	return nil
}

func (c *syncCore) GetStream(_ context.Context, name string) (*domain.StreamSpec, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, ok := c.streams[name]
	if !ok {
		return nil, domain.ErrStreamNotFound
	}
	return &s, nil
}

func (c *syncCore) GetStreams(context.Context) ([]domain.StreamSpec, error) { return nil, nil }

func (c *syncCore) Watch(context.Context, chan domain.StreamSpecWatchEvent) error { return nil }

func (c *syncCore) names() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	names := make([]string, 0, len(c.streams))
	for name := range c.streams {
		names = append(names, name)
	}
	return names
}

func runOperator(t *testing.T, core *syncCore, opts k8scrd.Options) {
	t.Helper()
	op, err := k8scrd.New(core, k8scrd.WithoutNameValidation(opts), slog.New(slog.DiscardHandler))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- op.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		assert.NoError(t, <-done)
	})
}

func requireStreams(t *testing.T, core *syncCore, want ...string) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.ElementsMatch(c, want, core.names())
	}, eventuallyTimeout, pollInterval)
	require.Never(t, func() bool {
		return !assert.ElementsMatch(new(assert.CollectT), want, core.names())
	}, settleTime, pollInterval)
}

func TestOperatorRunsOnlySelectedStreams(t *testing.T) {
	t.Parallel()
	c := startCluster(t)

	watched, other := c.newNamespace(t), c.newNamespace(t)
	selected := map[string]string{"tram.io/validation": "true"}
	c.createStream(t, watched, "selected", selected)
	c.createStream(t, watched, "unlabeled", nil)
	c.createStream(t, other, "selected", selected)

	core := newSyncCore()
	runOperator(t, core, k8scrd.Options{
		Kubeconfig:     c.adminKubeconfig(t),
		Namespaces:     []string{watched},
		StreamSelector: "tram.io/validation=true",
	})

	requireStreams(t, core, watched+".selected")
	c.requireActive(t, watched, "selected")

	c.setLabels(t, watched, "selected", nil)
	c.setLabels(t, watched, "unlabeled", selected)
	requireStreams(t, core, watched+".unlabeled")
	c.requireActive(t, watched, "unlabeled")
}

func TestOperatorsShareStreamsByLabelKeys(t *testing.T) {
	t.Parallel()
	c := startCluster(t)

	ns := c.newNamespace(t)
	c.createStream(t, ns, "only-validation", map[string]string{"tram.io/validation": "true"})
	c.createStream(t, ns, "only-bridge", map[string]string{"tram.io/bridge": "true"})
	c.createStream(t, ns, "shared", map[string]string{"tram.io/validation": "true", "tram.io/bridge": "true"})

	cfg := c.adminKubeconfig(t)
	validation, bridge := newSyncCore(), newSyncCore()
	runOperator(t, validation, k8scrd.Options{
		Kubeconfig: cfg, Namespaces: []string{ns}, StreamSelector: "tram.io/validation=true",
	})
	runOperator(t, bridge, k8scrd.Options{
		Kubeconfig: cfg, Namespaces: []string{ns}, StreamSelector: "tram.io/bridge=true",
	})

	requireStreams(t, validation, ns+".only-validation", ns+".shared")
	requireStreams(t, bridge, ns+".only-bridge", ns+".shared")
}

func TestOperatorNeedsOnlyRoleBindingInWatchedNamespace(t *testing.T) {
	t.Parallel()
	c := startCluster(t)

	content, err := os.ReadFile(filepath.Join("config", "rbac", "role.yaml"))
	require.NoError(t, err)
	var role rbacv1.ClusterRole
	require.NoError(t, yaml.Unmarshal(content, &role))
	require.NoError(t, c.admin.Create(t.Context(), &role))

	ns := c.newNamespace(t)
	const user = "tram-operator"
	require.NoError(t, c.admin.Create(t.Context(), &rbacv1.RoleBinding{
		Namespace: ns, Name: "tram",
		RoleRef:  rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: role.Name},
		Subjects: []rbacv1.Subject{{APIGroup: rbacv1.GroupName, Kind: rbacv1.UserKind, Name: user}},
	}))
	c.createStream(t, ns, "s", nil)

	core := newSyncCore()
	runOperator(t, core, k8scrd.Options{
		Kubeconfig: c.kubeconfig(t, envtest.User{Name: user}),
		Namespaces: []string{ns},
	})

	requireStreams(t, core, ns+".s")
	c.requireActive(t, ns, "s")
}
