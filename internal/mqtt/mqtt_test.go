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

package mqtt

import (
	"testing"

	"github.com/onsi/gomega"
)

func TestClientOptionsRegistersNoWill(t *testing.T) {
	t.Parallel()

	cases := []struct {
		desc     string
		address  string
		id       string
		username string
		password string
	}{
		{
			desc:     "a fully configured connection registers no will",
			address:  "tcp://localhost:1883",
			id:       "propeller-controller",
			username: "entity-1",
			password: "key-1",
		},
		{
			desc:     "an empty credential set still registers no will",
			address:  "tcp://localhost:1883",
			id:       "propeller-controller",
			username: "",
			password: "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()
			g := gomega.NewWithT(t)

			opts := clientOptions(tc.address, tc.id, tc.username, tc.password)

			g.Expect(opts.WillEnabled).To(gomega.BeFalse(),
				"the operator is not a proplet and must not announce one dying")
			g.Expect(opts.WillTopic).To(gomega.BeEmpty())
			g.Expect(opts.WillPayload).To(gomega.BeEmpty())
		})
	}
}

func TestClientOptionsConnectionSettings(t *testing.T) {
	t.Parallel()

	g := gomega.NewWithT(t)
	opts := clientOptions("tcp://localhost:1883", "propeller-controller", "entity-1", "key-1")

	g.Expect(opts.ClientID).To(gomega.Equal("propeller-controller"))
	g.Expect(opts.Username).To(gomega.Equal("entity-1"))
	g.Expect(opts.Password).To(gomega.Equal("key-1"))
	g.Expect(opts.CleanSession).To(gomega.BeTrue())
	g.Expect(opts.AutoReconnect).To(gomega.BeTrue())
	g.Expect(opts.Servers).To(gomega.HaveLen(1))
	g.Expect(opts.Servers[0].Host).To(gomega.Equal("localhost:1883"))
}
