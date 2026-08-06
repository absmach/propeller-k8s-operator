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
	"testing"

	propellerv1 "github.com/absmach/propeller/api/v1"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const testNamespace = "default"

func propletWithRPC(rpc *propellerv1.RPCSpec) *propellerv1.Proplet {
	return &propellerv1.Proplet{
		ObjectMeta: metav1.ObjectMeta{Name: "p1", Namespace: testNamespace},
		Spec: propellerv1.PropletSpec{
			Type: propellerv1.K8sProplet,
			K8s: &propellerv1.K8sPropletSpec{
				Image: "propeller/proplet:latest",
				RPC:   rpc,
			},
		},
	}
}

func secretRef() *corev1.SecretKeySelector {
	return &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: "proplet-rpc"},
		Key:                  "token",
	}
}

func TestRPCPort(t *testing.T) {
	t.Parallel()

	cases := []struct {
		desc string
		rpc  *propellerv1.RPCSpec
		want int32
	}{
		{
			desc: "rpc disabled falls back to the default port",
			rpc:  nil,
			want: defaultRPCPort,
		},
		{
			desc: "an unset port falls back to the default",
			rpc:  &propellerv1.RPCSpec{Enabled: true, TokenSecretRef: secretRef()},
			want: defaultRPCPort,
		},
		{
			desc: "an explicit port is honoured",
			rpc:  &propellerv1.RPCSpec{Enabled: true, Port: 9500, TokenSecretRef: secretRef()},
			want: 9500,
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()
			g := gomega.NewWithT(t)

			g.Expect(rpcPort(propletWithRPC(tc.rpc))).To(gomega.Equal(tc.want))
		})
	}
}

func TestRPCEnvVars(t *testing.T) {
	t.Parallel()

	cases := []struct {
		desc      string
		rpc       *propellerv1.RPCSpec
		wantNames []string
		wantPort  string
		wantToken bool
	}{
		{
			desc:      "no rpc spec yields no env",
			rpc:       nil,
			wantNames: nil,
		},
		{
			desc:      "disabled rpc yields no env",
			rpc:       &propellerv1.RPCSpec{Enabled: false, TokenSecretRef: secretRef()},
			wantNames: nil,
		},
		{
			desc: "enabled rpc binds all interfaces and wires the token",
			rpc:  &propellerv1.RPCSpec{Enabled: true, TokenSecretRef: secretRef()},
			wantNames: []string{
				envRPCEnabled,
				envRPCPort,
				envRPCBindAddress,
				envRPCToken,
			},
			wantPort:  "9094",
			wantToken: true,
		},
		{
			desc: "an explicit port is rendered",
			rpc:  &propellerv1.RPCSpec{Enabled: true, Port: 9500, TokenSecretRef: secretRef()},
			wantNames: []string{
				envRPCEnabled,
				envRPCPort,
				envRPCBindAddress,
				envRPCToken,
			},
			wantPort:  "9500",
			wantToken: true,
		},
		{
			desc: "a missing token secret leaves the token unset",
			rpc:  &propellerv1.RPCSpec{Enabled: true},
			wantNames: []string{
				envRPCEnabled,
				envRPCPort,
				envRPCBindAddress,
			},
			wantPort: "9094",
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()
			g := gomega.NewWithT(t)

			env := rpcEnvVars(propletWithRPC(tc.rpc))
			if tc.wantNames == nil {
				g.Expect(env).To(gomega.BeEmpty())

				return
			}

			names := make([]string, 0, len(env))
			byName := make(map[string]corev1.EnvVar, len(env))
			for _, e := range env {
				names = append(names, e.Name)
				byName[e.Name] = e
			}
			g.Expect(names).To(gomega.Equal(tc.wantNames))
			g.Expect(byName[envRPCPort].Value).To(gomega.Equal(tc.wantPort))
			g.Expect(byName[envRPCBindAddress].Value).To(gomega.Equal(rpcBindAddress))

			token, ok := byName[envRPCToken]
			g.Expect(ok).To(gomega.Equal(tc.wantToken))
			if tc.wantToken {
				g.Expect(token.ValueFrom).NotTo(gomega.BeNil())
				g.Expect(token.ValueFrom.SecretKeyRef).NotTo(gomega.BeNil())
				g.Expect(token.ValueFrom.SecretKeyRef.Name).To(gomega.Equal("proplet-rpc"))
				g.Expect(token.ValueFrom.SecretKeyRef.Key).To(gomega.Equal("token"))
				g.Expect(token.Value).To(gomega.BeEmpty())
			}
		})
	}
}

func TestRPCContainerPorts(t *testing.T) {
	t.Parallel()

	cases := []struct {
		desc     string
		rpc      *propellerv1.RPCSpec
		wantPort int32
		wantNone bool
	}{
		{
			desc:     "disabled rpc exposes no port",
			rpc:      nil,
			wantNone: true,
		},
		{
			desc:     "enabled rpc exposes the default port",
			rpc:      &propellerv1.RPCSpec{Enabled: true, TokenSecretRef: secretRef()},
			wantPort: defaultRPCPort,
		},
		{
			desc:     "enabled rpc exposes an explicit port",
			rpc:      &propellerv1.RPCSpec{Enabled: true, Port: 9500, TokenSecretRef: secretRef()},
			wantPort: 9500,
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()
			g := gomega.NewWithT(t)

			ports := rpcContainerPorts(propletWithRPC(tc.rpc))
			if tc.wantNone {
				g.Expect(ports).To(gomega.BeEmpty())

				return
			}

			g.Expect(ports).To(gomega.HaveLen(1))
			g.Expect(ports[0].ContainerPort).To(gomega.Equal(tc.wantPort))
			g.Expect(ports[0].Name).To(gomega.Equal(rpcPortName))
			g.Expect(ports[0].Protocol).To(gomega.Equal(corev1.ProtocolTCP))
		})
	}
}

func TestBuildPropletDeploymentRPCWiring(t *testing.T) {
	t.Parallel()

	cases := []struct {
		desc        string
		rpc         *propellerv1.RPCSpec
		wantRPCEnv  bool
		wantRPCPort bool
	}{
		{
			desc:        "an rpc free proplet keeps its original env and no ports",
			rpc:         nil,
			wantRPCEnv:  false,
			wantRPCPort: false,
		},
		{
			desc:        "an rpc proplet gains env and a container port",
			rpc:         &propellerv1.RPCSpec{Enabled: true, TokenSecretRef: secretRef()},
			wantRPCEnv:  true,
			wantRPCPort: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()
			g := gomega.NewWithT(t)

			r := &PropletReconciler{}
			proplet := propletWithRPC(tc.rpc)
			proplet.Spec.ConnectionConfig = propellerv1.ConnectionConfig{
				DomainID:    "tenant-1",
				ChannelID:   "channel-1",
				ClientID:    "entity-1",
				ClientKey:   "key-1",
				MQTTAddress: "tcp://localhost:1883",
				MQTTTimeout: &metav1.Duration{},
			}

			deployment := r.buildPropletDeployment(proplet)
			g.Expect(deployment.Spec.Template.Spec.Containers).To(gomega.HaveLen(1))
			container := deployment.Spec.Template.Spec.Containers[0]

			names := make(map[string]bool, len(container.Env))
			for _, e := range container.Env {
				names[e.Name] = true
			}

			g.Expect(names["PROPLET_TENANT_ID"]).To(gomega.BeTrue(), "tenant id env must always be set")
			g.Expect(names["PROPLET_ENTITY_ID"]).To(gomega.BeTrue(), "entity id env must always be set")
			g.Expect(names["PROPLET_API_KEY"]).To(gomega.BeTrue(), "api key env must always be set")
			g.Expect(names[envRPCEnabled]).To(gomega.Equal(tc.wantRPCEnv))

			if tc.wantRPCPort {
				g.Expect(container.Ports).To(gomega.HaveLen(1))
				g.Expect(container.Ports[0].ContainerPort).To(gomega.Equal(int32(defaultRPCPort)))

				return
			}
			g.Expect(container.Ports).To(gomega.BeEmpty())
		})
	}
}

func TestBuildPropletService(t *testing.T) {
	t.Parallel()

	cases := []struct {
		desc     string
		rpc      *propellerv1.RPCSpec
		wantPort int32
	}{
		{
			desc:     "service uses the default port",
			rpc:      &propellerv1.RPCSpec{Enabled: true, TokenSecretRef: secretRef()},
			wantPort: defaultRPCPort,
		},
		{
			desc:     "service uses an explicit port",
			rpc:      &propellerv1.RPCSpec{Enabled: true, Port: 9500, TokenSecretRef: secretRef()},
			wantPort: 9500,
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()
			g := gomega.NewWithT(t)

			r := &PropletReconciler{}
			svc := r.buildPropletService(propletWithRPC(tc.rpc))

			g.Expect(svc.Name).To(gomega.Equal("p1-rpc"))
			g.Expect(svc.Namespace).To(gomega.Equal(testNamespace))
			g.Expect(svc.Spec.Type).To(gomega.Equal(corev1.ServiceTypeClusterIP))
			g.Expect(svc.Spec.Selector).To(gomega.Equal(map[string]string{appLabelKey: "p1"}))
			g.Expect(svc.Spec.Ports).To(gomega.HaveLen(1))
			g.Expect(svc.Spec.Ports[0].Port).To(gomega.Equal(tc.wantPort))
			g.Expect(svc.Spec.Ports[0].TargetPort.IntVal).To(gomega.Equal(tc.wantPort))
		})
	}
}
