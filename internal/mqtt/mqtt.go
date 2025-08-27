package mqtt

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	connTimeout    = 10
	reconnTimeout  = 1
	disconnTimeout = 250
)

var (
	errPublishTimeout     = errors.New("failed to publish due to timeout reached")
	errSubscribeTimeout   = errors.New("failed to subscribe due to timeout reached")
	errUnsubscribeTimeout = errors.New("failed to unsubscribe due to timeout reached")
	errEmptyTopic         = errors.New("empty topic")
	errEmptyClientID      = errors.New("empty client ID")

	aliveTopicTemplate = "m/%s/c/%s/messages/control/proplet/alive"
	lwtPayloadTemplate = `{"status":"offline","proplet_id":"%s","smq_channel_id":"%s"}`
	mqttLogger         = logf.Log.WithName("mqtt")
)

type pubsub struct {
	client  mqtt.Client
	qos     byte
	timeout time.Duration
}

type Handler func(topic string, msg map[string]any) error

type PubSub interface {
	Publish(topic string, msg any) error
	Subscribe(topic string, handler Handler) error
	Unsubscribe(topic string) error
	Disconnect() error
}

func NewPubSub(url string, qos byte, id, username, password, domainID, channelID string, timeout time.Duration) (PubSub, error) {
	if id == "" {
		return nil, errEmptyClientID
	}

	client, err := newClient(url, id, username, password, domainID, channelID, timeout)
	if err != nil {
		return nil, err
	}

	return &pubsub{
		client:  client,
		qos:     qos,
		timeout: timeout,
	}, nil
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

	return nil
}

func (ps *pubsub) Disconnect() error {
	ps.client.Disconnect(disconnTimeout)

	return nil
}

func newClient(address, id, username, password, domainID, channelID string, timeout time.Duration) (mqtt.Client, error) {
	opts := mqtt.NewClientOptions().
		AddBroker(address).
		SetClientID(id).
		SetUsername(username).
		SetPassword(password).
		SetCleanSession(true).
		SetAutoReconnect(true).
		SetConnectTimeout(connTimeout * time.Second).
		SetMaxReconnectInterval(reconnTimeout * time.Minute)

	if channelID != "" {
		topic := fmt.Sprintf(aliveTopicTemplate, domainID, channelID)
		lwtPayload := fmt.Sprintf(lwtPayloadTemplate, username, channelID)
		opts.SetWill(topic, lwtPayload, 0, false)
	}

	opts.SetOnConnectHandler(func(_ mqtt.Client) {
		mqttLogger.Info("MQTT connection lost")
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
