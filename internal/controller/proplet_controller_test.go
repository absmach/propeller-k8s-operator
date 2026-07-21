/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	propellerv1 "github.com/absmach/propeller/api/v1"
)

var _ = Describe("Proplet Controller", func() {
	Context("When reconciling a resource", func() {
		const resourceName = "test-resource"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default", // TODO(user):Modify as needed
		}
		proplet := &propellerv1.Proplet{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind Proplet")
			err := k8sClient.Get(ctx, typeNamespacedName, proplet)
			if err != nil && errors.IsNotFound(err) {
				resource := &propellerv1.Proplet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: "default",
					},
					Spec: propellerv1.PropletSpec{
						Type: propellerv1.PropletKind("k8s"),
						K8s: &propellerv1.K8sPropletSpec{
							Image: "test-image",
						},
						ConnectionConfig: propellerv1.ConnectionConfig{
							EntityID:    "test-entity",
							APIKey:      "test-key",
							TenantID:    "test-tenant",
							ChannelID:   "test-channel",
							MQTTAddress: "tcp://localhost:1883",
						},
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			// TODO(user): Cleanup logic after each test, like removing the resource instance.
			resource := &propellerv1.Proplet{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance Proplet")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})
		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			controllerReconciler := &PropletReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			// TODO(user): Add more specific assertions depending on your controller's reconciliation logic.
			// Example: If you expect a certain status condition after reconciliation, verify it here.
		})
	})
})
