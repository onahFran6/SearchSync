package platform

import (
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// NewKafkaConsumer creates a consumer-group client with autocommit disabled;
// callers must call CommitRecords explicitly after successfully processing
// each record. AtStart only applies the first time this group runs, so a
// fresh indexer backfills from the topic's retained history (including the
// Debezium snapshot) instead of only seeing events published after startup.
func NewKafkaConsumer(brokers []string, group string, topics []string) (*kgo.Client, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka consumer: %w", err)
	}
	return client, nil
}
