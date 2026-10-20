package platform

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// sensitiveJSONFields is a defensive deny-list applied to any payload
// before it is written to a DLQ topic. Neither existing event schema
// (products rows, OrderCreated) carries anything in this list today — see
// internal/orders/order.go's comment on deliberately excluding payment
// data — but the DLQ is a second place these payloads get persisted
// (Elasticsearch/Postgres don't see arbitrary future fields a producer
// might add), so redaction happens here rather than trusting every future
// producer to remember.
var sensitiveJSONFields = []string{
	"password", "secret", "token", "apiKey", "api_key",
	"ssn", "creditCard", "credit_card", "cvv", "email",
}

// RedactJSON returns a copy of a JSON object with any top-level field whose
// name matches sensitiveJSONFields (case-insensitive) replaced with
// "REDACTED". Non-object payloads, or payloads that fail to parse as an
// object, are returned unchanged — the DLQ must never lose the original
// bytes needed to diagnose a decode failure.
func RedactJSON(raw []byte) []byte {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return raw
	}
	redacted := false
	for _, field := range sensitiveJSONFields {
		for key := range obj {
			if strings.EqualFold(key, field) {
				obj[key] = json.RawMessage(`"REDACTED"`)
				redacted = true
			}
		}
	}
	if !redacted {
		return raw
	}
	out, err := json.Marshal(obj)
	if err != nil {
		return raw
	}
	return out
}

// DLQRecord is the diagnostic envelope written to a dead-letter topic for a
// record a consumer could not apply, whether because it was permanently
// unprocessable or because its retry budget was exhausted.
type DLQRecord struct {
	OriginalTopic     string
	OriginalPartition int32
	OriginalOffset    int64
	Key               []byte
	Value             []byte
	Reason            string
	Attempts          int
	FailedAt          time.Time
	ConsumerGroup     string
}

// DLQProducer durably publishes DLQRecords to a dead-letter Kafka topic.
// "Durably" means Publish does not return until the broker has
// acknowledged the record — a consumer must only commit a quarantined
// record's original offset after Publish returns nil.
type DLQProducer struct {
	client *kgo.Client
	topic  string
}

// NewDLQProducer creates a producer-only client for a dead-letter topic.
// Idempotent producing is enabled so a retried Publish (after a transient
// produce failure) cannot duplicate a DLQ entry.
func NewDLQProducer(brokers []string, topic string) (*DLQProducer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return nil, fmt.Errorf("create dlq producer for topic %s: %w", topic, err)
	}
	return &DLQProducer{client: client, topic: topic}, nil
}

func (p *DLQProducer) Close() {
	p.client.Close()
}

// Publish writes rec to the dead-letter topic synchronously, redacting the
// payload before it leaves process memory. The original key is preserved
// so a DLQ record for a given business key can be found without scanning
// the whole topic.
func (p *DLQProducer) Publish(ctx context.Context, rec DLQRecord) error {
	record := &kgo.Record{
		Topic: p.topic,
		Key:   rec.Key,
		Value: RedactJSON(rec.Value),
		Headers: []kgo.RecordHeader{
			{Key: "original-topic", Value: []byte(rec.OriginalTopic)},
			{Key: "original-partition", Value: []byte(strconv.FormatInt(int64(rec.OriginalPartition), 10))},
			{Key: "original-offset", Value: []byte(strconv.FormatInt(rec.OriginalOffset, 10))},
			{Key: "reason", Value: []byte(rec.Reason)},
			{Key: "attempts", Value: []byte(strconv.Itoa(rec.Attempts))},
			{Key: "failed-at", Value: []byte(rec.FailedAt.UTC().Format(time.RFC3339Nano))},
			{Key: "consumer-group", Value: []byte(rec.ConsumerGroup)},
		},
	}

	results := p.client.ProduceSync(ctx, record)
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("publish dlq record for %s[%d]@%d: %w",
			rec.OriginalTopic, rec.OriginalPartition, rec.OriginalOffset, err)
	}
	return nil
}
