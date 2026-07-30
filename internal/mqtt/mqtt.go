package mqtt

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	connTimeout    = 10
	reconnTimeout  = 1
	disconnTimeout = 250
	// keepAlive is set below paho's 30s default: some network paths (e.g. a
	// relay/tunnel) treat a connection as idle and terminate it before that
	// default's next PINGREQ is due, causing frequent reconnects.
	keepAlive   = 10 * time.Second
	pingTimeout = 5 * time.Second
)

var (
	errPublishTimeout     = errors.New("failed to publish due to timeout reached")
	errSubscribeTimeout   = errors.New("failed to subscribe due to timeout reached")
	errUnsubscribeTimeout = errors.New("failed to unsubscribe due to timeout reached")
	errEmptyTopic         = errors.New("empty topic")
	errEmptyClientID      = errors.New("empty client ID")

	aliveTopicTemplate = "m/%s/c/%s/control/proplet/alive"
	lwtPayloadTemplate = `{"status":"offline","proplet_id":"%s","channel_id":"%s"}`
	mqttLogger         = logf.Log.WithName("mqtt")
)

type pubsub struct {
	client  mqtt.Client
	qos     byte
	timeout time.Duration

	// subsMu guards subs, which records every topic currently subscribed
	// via Subscribe so resubscribeAll can restore them after a reconnect.
	// SetCleanSession(true) means the broker forgets our subscriptions
	// (and anything published while we were disconnected) on every
	// reconnect, so without this the client would silently stop receiving
	// messages after the first connection blip.
	subsMu sync.Mutex
	subs   map[string]Handler
}

type Handler func(topic string, msg map[string]any) error

type PubSub interface {
	Publish(topic string, msg any) error
	Subscribe(topic string, handler Handler) error
	Unsubscribe(topic string) error
	Disconnect() error
}

func NewPubSub(url string, qos byte, id, username, password, tenantID, channelID string, timeout time.Duration) (PubSub, error) {
	if id == "" {
		return nil, errEmptyClientID
	}

	ps := &pubsub{
		qos:     qos,
		timeout: timeout,
		subs:    make(map[string]Handler),
	}

	client, err := newClient(url, id, username, password, tenantID, channelID, timeout, ps.resubscribeAll)
	if err != nil {
		return nil, err
	}
	ps.client = client

	return ps, nil
}

func (ps *pubsub) resubscribeAll(client mqtt.Client) {
	ps.subsMu.Lock()
	defer ps.subsMu.Unlock()

	for topic, handler := range ps.subs {
		token := client.Subscribe(topic, ps.qos, ps.mqttHandler(handler))
		go func(topic string, token mqtt.Token) {
			if ok := token.WaitTimeout(ps.timeout); !ok {
				mqttLogger.Error(errSubscribeTimeout, "failed to resubscribe after reconnect", "topic", topic)
				return
			}
			if err := token.Error(); err != nil {
				mqttLogger.Error(err, "failed to resubscribe after reconnect", "topic", topic)
				return
			}
			mqttLogger.Info("resubscribed after reconnect", "topic", topic)
		}(topic, token)
	}
}

func (ps *pubsub) Publish(topic string, msg any) error {
	if topic == "" {
		return errEmptyTopic
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	token := ps.client.Publish(topic, ps.qos, false, data)
	if token.Error() != nil {
		return token.Error()
	}

	if ok := token.WaitTimeout(ps.timeout); !ok {
		return errPublishTimeout
	}

	return nil
}

func (ps *pubsub) Subscribe(topic string, handler Handler) error {
	if topic == "" {
		return errEmptyTopic
	}

	token := ps.client.Subscribe(topic, ps.qos, ps.mqttHandler(handler))
	if token.Error() != nil {
		return token.Error()
	}
	if ok := token.WaitTimeout(ps.timeout); !ok {
		return errSubscribeTimeout
	}

	ps.subsMu.Lock()
	ps.subs[topic] = handler
	ps.subsMu.Unlock()

	return nil
}

func (ps *pubsub) Unsubscribe(topic string) error {
	if topic == "" {
		return errEmptyTopic
	}

	token := ps.client.Unsubscribe(topic)
	if token.Error() != nil {
		return token.Error()
	}

	if ok := token.WaitTimeout(ps.timeout); !ok {
		return errUnsubscribeTimeout
	}

	ps.subsMu.Lock()
	delete(ps.subs, topic)
	ps.subsMu.Unlock()

	return nil
}

func (ps *pubsub) Disconnect() error {
	ps.client.Disconnect(disconnTimeout)

	return nil
}

func newClient(address, id, username, password, tenantID, channelID string, timeout time.Duration, onConnect func(mqtt.Client)) (mqtt.Client, error) {
	opts := mqtt.NewClientOptions().
		AddBroker(address).
		SetClientID(id).
		SetUsername(username).
		SetPassword(password).
		SetCleanSession(true).
		SetAutoReconnect(true).
		SetConnectTimeout(connTimeout * time.Second).
		SetMaxReconnectInterval(reconnTimeout * time.Minute).
		SetKeepAlive(keepAlive).
		SetPingTimeout(pingTimeout)

	if channelID != "" {
		topic := fmt.Sprintf(aliveTopicTemplate, tenantID, channelID)
		// Use the MQTT client ID (id) as proplet_id — this matches the field
		// the PropletReconciler's alive handler matches against
		// Spec.ConnectionConfig.EntityID.  username is the API key/password,
		// not the entity identity.
		lwtPayload := fmt.Sprintf(lwtPayloadTemplate, id, channelID)
		opts.SetWill(topic, lwtPayload, 0, false)
	}

	opts.SetOnConnectHandler(func(c mqtt.Client) {
		mqttLogger.Info("MQTT connected")
		onConnect(c)
	})

	opts.SetConnectionLostHandler(func(_ mqtt.Client, err error) {
		mqttLogger.Error(err, "MQTT connection lost")
	})

	opts.SetReconnectingHandler(func(_ mqtt.Client, options *mqtt.ClientOptions) {
		args := []any{}
		if options != nil {
			args = append(args,
				"client_id", options.ClientID,
				"username", options.Username,
			)
		}

		mqttLogger.Info("MQTT reconnecting", args...)
	})

	client := mqtt.NewClient(opts)

	token := client.Connect()
	if token.Error() != nil {
		return nil, errors.Join(errors.New("failed to connect to MQTT broker"), token.Error())
	}

	if ok := token.WaitTimeout(timeout); !ok {
		return nil, errors.New("timeout reached while connecting to MQTT broker")
	}

	return client, nil
}

func (ps *pubsub) mqttHandler(h Handler) mqtt.MessageHandler {
	return func(_ mqtt.Client, m mqtt.Message) {
		var msg map[string]any
		if err := json.Unmarshal(m.Payload(), &msg); err != nil {
			mqttLogger.Error(err, "Failed to unmarshal received message", "topic", m.Topic(), "payload", string(m.Payload()))

			return
		}

		if err := h(m.Topic(), msg); err != nil {
			mqttLogger.Error(err, "Failed to handle MQTT message", "topic", m.Topic(), "payload", msg)

			return
		}

		m.Ack()
	}
}
