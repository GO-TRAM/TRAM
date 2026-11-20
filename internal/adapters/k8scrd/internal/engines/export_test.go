// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package engines

import (
	"time"

	appsv1 "k8s.io/api/apps/v1"
	appsv1ac "k8s.io/client-go/applyconfigurations/apps/v1"
	autoscalingv2ac "k8s.io/client-go/applyconfigurations/autoscaling/v2"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	policyv1ac "k8s.io/client-go/applyconfigurations/policy/v1"
	rbacv1ac "k8s.io/client-go/applyconfigurations/rbac/v1"

	tramv1 "github.com/go-tram/tram/internal/adapters/k8scrd/api/v1"
	tramv1alpha1 "github.com/go-tram/tram/internal/adapters/k8scrd/api/v1alpha1"
)

// Resources exposes the apply configurations of an Engine's resources.
type Resources struct{ res resources }

func (r Resources) ServiceAccount() *corev1ac.ServiceAccountApplyConfiguration {
	return r.res.serviceAccount
}
func (r Resources) RoleBinding() *rbacv1ac.RoleBindingApplyConfiguration { return r.res.roleBinding }
func (r Resources) Deployment() *appsv1ac.DeploymentApplyConfiguration   { return r.res.deployment }
func (r Resources) Service() *corev1ac.ServiceApplyConfiguration         { return r.res.service }

func (r Resources) HPA() *autoscalingv2ac.HorizontalPodAutoscalerApplyConfiguration {
	return r.res.hpa
}

func (r Resources) PDB() *policyv1ac.PodDisruptionBudgetApplyConfiguration { return r.res.pdb }

func BuildResources(
	engine *tramv1alpha1.Engine,
	image, clusterRole string,
	replicas *int32,
) (Resources, error) {
	res, err := buildResources(engine, buildParams{image: image, clusterRole: clusterRole, replicas: replicas})
	return Resources{res: res}, err
}

func Autoscaled(engine *tramv1alpha1.Engine) bool { return autoscaled(engine) }

func OwnsReplicas(deployment *appsv1.Deployment, manager string) bool {
	return ownsReplicas(deployment, manager)
}

func ClaimOrdinal(engine, claim string) (int, bool) { return claimOrdinal(engine, claim) }

func MaxBufferAge(streams []tramv1.Stream) time.Duration { return maxBufferAge(streams) }
