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

const (
	rpcTestNamespace = "default"
	rpcTokenSecret   = "proplet-rpc"
	rpcTokenKey      = "token"
)

func rpcSecretRef() *corev1.SecretKeySelector {
	return &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: rpcTokenSecret},
		Key:                  rpcTokenKey,
	}
}

func propletWithRPC(rpc *propellerv1.RPCSpec) *propellerv1.Proplet {
	return &propellerv1.Proplet{
		ObjectMeta: metav1.ObjectMeta{Name: "p1", Namespace: rpcTestNamespace},
		Spec: propellerv1.PropletSpec{
			Type: propellerv1.K8sProplet,
			K8s: &propellerv1.K8sPropletSpec{
				Image:    "propeller/proplet:latest",
				LogLevel: "info",
				RPC:      rpc,
			},
			ConnectionConfig: propellerv1.ConnectionConfig{
				TenantID:    "tenant-1",
				ChannelID:   "channel-1",
				EntityID:    "entity-1",
				APIKey:      "key-1",
				MQTTAddress: "tcp://localhost:1883",
				MQTTTimeout: &metav1.Duration{},
			},
		},
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
			rpc:  &propellerv1.RPCSpec{Enabled: true, TokenSecretRef: rpcSecretRef()},
			want: defaultRPCPort,
		},
		{
			desc: "an explicit port is honoured",
			rpc:  &propellerv1.RPCSpec{Enabled: true, Port: 9500, TokenSecretRef: rpcSecretRef()},
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
			rpc:       &propellerv1.RPCSpec{Enabled: false, TokenSecretRef: rpcSecretRef()},
			wantNames: nil,
		},
		{
			desc:      "enabled rpc binds all interfaces and wires the token",
			rpc:       &propellerv1.RPCSpec{Enabled: true, TokenSecretRef: rpcSecretRef()},
			wantNames: []string{envRPCEnabled, envRPCPort, envRPCBindAddress, envRPCToken},
			wantPort:  "9094",
			wantToken: true,
		},
		{
			desc:      "an explicit port is rendered",
			rpc:       &propellerv1.RPCSpec{Enabled: true, Port: 9500, TokenSecretRef: rpcSecretRef()},
			wantNames: []string{envRPCEnabled, envRPCPort, envRPCBindAddress, envRPCToken},
			wantPort:  "9500",
			wantToken: true,
		},
		{
			desc:      "a missing token secret leaves the token unset",
			rpc:       &propellerv1.RPCSpec{Enabled: true},
			wantNames: []string{envRPCEnabled, envRPCPort, envRPCBindAddress},
			wantPort:  "9094",
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
				g.Expect(token.ValueFrom.SecretKeyRef.Name).To(gomega.Equal(rpcTokenSecret))
				g.Expect(token.ValueFrom.SecretKeyRef.Key).To(gomega.Equal(rpcTokenKey))
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
			rpc:      &propellerv1.RPCSpec{Enabled: true, TokenSecretRef: rpcSecretRef()},
			wantPort: defaultRPCPort,
		},
		{
			desc:     "enabled rpc exposes an explicit port",
			rpc:      &propellerv1.RPCSpec{Enabled: true, Port: 9500, TokenSecretRef: rpcSecretRef()},
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

func TestBuildPropletEnvRPCWiring(t *testing.T) {
	t.Parallel()

	cases := []struct {
		desc       string
		rpc        *propellerv1.RPCSpec
		wantRPCEnv bool
	}{
		{
			desc:       "an rpc free proplet keeps its base env only",
			rpc:        nil,
			wantRPCEnv: false,
		},
		{
			desc:       "an rpc proplet gains the rpc env",
			rpc:        &propellerv1.RPCSpec{Enabled: true, TokenSecretRef: rpcSecretRef()},
			wantRPCEnv: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()
			g := gomega.NewWithT(t)

			names := map[string]bool{}
			for _, e := range buildPropletEnv(propletWithRPC(tc.rpc)) {
				names[e.Name] = true
			}

			g.Expect(names["PROPLET_TENANT_ID"]).To(gomega.BeTrue())
			g.Expect(names["PROPLET_ENTITY_ID"]).To(gomega.BeTrue())
			g.Expect(names[envRPCEnabled]).To(gomega.Equal(tc.wantRPCEnv))
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
			rpc:      &propellerv1.RPCSpec{Enabled: true, TokenSecretRef: rpcSecretRef()},
			wantPort: defaultRPCPort,
		},
		{
			desc:     "service uses an explicit port",
			rpc:      &propellerv1.RPCSpec{Enabled: true, Port: 9500, TokenSecretRef: rpcSecretRef()},
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
			g.Expect(svc.Namespace).To(gomega.Equal(rpcTestNamespace))
			g.Expect(svc.Spec.Type).To(gomega.Equal(corev1.ServiceTypeClusterIP))
			g.Expect(svc.Spec.Selector).To(gomega.Equal(map[string]string{"app": "p1"}))
			g.Expect(svc.Spec.Ports).To(gomega.HaveLen(1))
			g.Expect(svc.Spec.Ports[0].Port).To(gomega.Equal(tc.wantPort))
			g.Expect(svc.Spec.Ports[0].TargetPort.IntVal).To(gomega.Equal(tc.wantPort))
		})
	}
}
