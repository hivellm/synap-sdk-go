package synap

import (
	"context"
	"encoding/json"
	"sync"
)

// PubSubManager provides pub/sub operations against the Synap server.
type PubSubManager struct {
	client *SynapClient
}

// Publish sends data to a topic. priority is 0–9 (0 = default).
// Returns the number of subscribers that received the message.
func (p *PubSubManager) Publish(ctx context.Context, topic string, data interface{}, priority uint8) (int, error) {
	type payload struct {
		Topic    string      `json:"topic"`
		Payload  interface{} `json:"payload"`
		Priority uint8       `json:"priority"`
	}
	raw, err := p.client.sendCommand(ctx, "pubsub.publish", payload{Topic: topic, Payload: data, Priority: priority})
	if err != nil {
		return 0, err
	}
	var result struct {
		SubscribersMatched int `json:"subscribers_matched"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return 0, newInvalidResponseError("pubsub.publish: " + err.Error())
	}
	return result.SubscribersMatched, nil
}

// Subscribe registers subscriberID to receive messages on the given topics.
// Wildcard patterns are supported: "user.*" (single-level), "user.#" (multi-level).
// Returns the subscriber ID confirmed by the server.
func (p *PubSubManager) Subscribe(ctx context.Context, subscriberID string, topics []string) (string, error) {
	type payload struct {
		SubscriberID string   `json:"subscriber_id"`
		Topics       []string `json:"topics"`
	}
	raw, err := p.client.sendCommand(ctx, "pubsub.subscribe", payload{SubscriberID: subscriberID, Topics: topics})
	if err != nil {
		return "", err
	}
	var result struct {
		SubscriberID   string `json:"subscriber_id"`
		SubscriptionID string `json:"subscription_id"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return "", newInvalidResponseError("pubsub.subscribe: " + err.Error())
	}
	if result.SubscriberID != "" {
		return result.SubscriberID, nil
	}
	return result.SubscriptionID, nil
}

// Unsubscribe removes subscriberID from the given topics.
func (p *PubSubManager) Unsubscribe(ctx context.Context, subscriberID string, topics []string) error {
	type payload struct {
		SubscriberID string   `json:"subscriber_id"`
		Topics       []string `json:"topics"`
	}
	_, err := p.client.sendCommand(ctx, "pubsub.unsubscribe", payload{SubscriberID: subscriberID, Topics: topics})
	return err
}

// ListTopics returns all active topics on the server.
func (p *PubSubManager) ListTopics(ctx context.Context) ([]string, error) {
	raw, err := p.client.sendCommand(ctx, "pubsub.topics", struct{}{})
	if err != nil {
		return nil, err
	}
	var result struct {
		Topics []string `json:"topics"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, newInvalidResponseError("pubsub.topics: " + err.Error())
	}
	return result.Topics, nil
}

// PushMessage is one message delivered on a push subscription.
type PushMessage struct {
	Topic     string
	Payload   string
	ID        string
	Timestamp int64
}

// Observe opens a dedicated push subscription and streams messages published
// to the given topics.
//
// This is only available on the `synap://` transport: server push is a
// SynapRPC capability, and Thunder routes push frames — the ones carrying the
// reserved id — to a hook rather than to a waiting call. HTTP and RESP3
// clients get ErrPushUnsupported.
//
// The push hook is registered before SUBSCRIBE is sent, so a message published
// between the server's acknowledgement and the reader starting cannot be lost.
// Cancel the context, or call the returned stop, to close the subscription;
// the channel is closed when it ends.
func (p *PubSubManager) Observe(
	ctx context.Context,
	topics []string,
) (messages <-chan PushMessage, subscriberID string, stop func(), err error) {
	if p.client.rpc == nil {
		return nil, "", nil, newServerError(
			"pub/sub push requires the synap:// transport; " +
				"this client is on HTTP or RESP3")
	}

	// Buffered so a slow consumer cannot block the connection's reader.
	out := make(chan PushMessage, 64)
	var once sync.Once

	subscriberID, cancel, err := p.client.rpc.SubscribePush(ctx, topics,
		func(frame map[string]interface{}) {
			msg := PushMessage{
				Topic:   asString(frame["topic"]),
				Payload: asString(frame["payload"]),
				ID:      asString(frame["id"]),
			}
			if ts, ok := frame["timestamp"].(int64); ok {
				msg.Timestamp = ts
			}
			select {
			case out <- msg:
			default:
				// Dropping is better than stalling the shared reader; a
				// consumer that cannot keep up loses the oldest frames.
			}
		})
	if err != nil {
		return nil, "", nil, err
	}

	shutdown := func() {
		once.Do(func() {
			cancel()
			close(out)
		})
	}

	// Cancelling the caller's context tears the subscription down too.
	go func() {
		<-ctx.Done()
		shutdown()
	}()

	return out, subscriberID, shutdown, nil
}

// asString renders a push-frame field that is expected to be textual.
func asString(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
