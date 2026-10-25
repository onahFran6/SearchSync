// Command replay reconsumes a bounded offset or time range from one of
// SearchSync's existing topics and reapplies it through the exact same
// handler the live consumer uses, so the existing idempotency guarantees (the
// processed_events unique constraint, Elasticsearch external versioning)
// are what make a replay safe, rather than a second ad-hoc dedup mechanism
// invented here.
//
// It never joins a consumer group: ranges are read via explicit partition
// seeks (kgo.ConsumePartitions), so a replay run can never perturb the live
// consumer's committed offsets, and never auto-commits anything of its own.
//
// Dry-run is the default. In dry-run, the tool decodes every record in
// range and prints what it would do, but never calls the Elasticsearch or
// Postgres side effect at all - not "calls it but discards the result",
// genuinely does not reach the dependency. Pass -dry-run=false to apply.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/onahFran6/SearchSync/internal/indexer"
	"github.com/onahFran6/SearchSync/internal/notifications"
	"github.com/onahFran6/SearchSync/internal/platform"
	"github.com/onahFran6/SearchSync/internal/search"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	cfg, err := parseFlags(os.Args[1:])
	if err != nil {
		logger.Error("invalid arguments", "error", err)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg, logger); err != nil {
		logger.Error("replay failed", "error", err)
		os.Exit(1)
	}
}

type config struct {
	target     string // "indexer" or "notifications"
	brokers    []string
	topic      string
	partition  int32  // -1 means "all partitions of topic"
	fromOffset int64  // -1 means unset
	toOffset   int64  // -1 means unset (resolved to current end offset)
	fromTime   string // RFC3339, mutually exclusive with fromOffset
	toTime     string // RFC3339, optional upper bound for fromTime mode
	dryRun     bool

	elasticsearchURL string
	productsIndex    string

	postgresDSN string
}

func parseFlags(args []string) (config, error) {
	fs := flag.NewFlagSet("replay", flag.ContinueOnError)

	target := fs.String("target", "", "which consumer's apply path to replay through: indexer or notifications (required)")
	brokers := fs.String("brokers", getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"), "comma-separated Kafka broker list")
	topic := fs.String("topic", "", "topic to replay from (defaults to the target's production topic)")
	partition := fs.Int("partition", -1, "single partition to replay (default: all partitions of the topic)")
	fromOffset := fs.Int64("from-offset", -1, "inclusive start offset on -partition (mutually exclusive with -from-time)")
	toOffset := fs.Int64("to-offset", -1, "exclusive end offset on -partition (default: partition's end offset at run start)")
	fromTime := fs.String("from-time", "", "RFC3339 inclusive start time, resolved to offsets via Kafka's ListOffsets (mutually exclusive with -from-offset)")
	toTime := fs.String("to-time", "", "RFC3339 exclusive end time (default: now, at run start)")
	dryRun := fs.Bool("dry-run", true, "decode and print what would happen without calling Elasticsearch/Postgres; pass -dry-run=false to apply")
	esURL := fs.String("elasticsearch-url", getenv("ELASTICSEARCH_URL", "http://localhost:9200"), "destination Elasticsearch URL (indexer target)")
	index := fs.String("products-index", getenv("PRODUCTS_INDEX", "products"), "destination Elasticsearch index (indexer target)")
	pgDSN := fs.String("postgres-dsn", "", "destination Postgres DSN (notifications target); defaults to POSTGRES_* env vars if empty")

	if err := fs.Parse(args); err != nil {
		return config{}, err
	}

	cfg := config{
		target:           *target,
		brokers:          strings.Split(*brokers, ","),
		topic:            *topic,
		partition:        int32(*partition),
		fromOffset:       *fromOffset,
		toOffset:         *toOffset,
		fromTime:         *fromTime,
		toTime:           *toTime,
		dryRun:           *dryRun,
		elasticsearchURL: *esURL,
		productsIndex:    *index,
		postgresDSN:      *pgDSN,
	}

	switch cfg.target {
	case "indexer":
		if cfg.topic == "" {
			cfg.topic = getenv("PRODUCTS_TOPIC", "searchsync.public.products")
		}
	case "notifications":
		if cfg.topic == "" {
			cfg.topic = getenv("ORDER_EVENTS_TOPIC", "searchsync.outbox.event.orders")
		}
	default:
		return config{}, fmt.Errorf("-target must be %q or %q, got %q", "indexer", "notifications", cfg.target)
	}

	haveOffsetRange := cfg.fromOffset >= 0
	haveTimeRange := cfg.fromTime != ""
	if haveOffsetRange == haveTimeRange {
		return config{}, fmt.Errorf("specify exactly one of -from-offset (with -partition) or -from-time")
	}
	if haveOffsetRange && cfg.partition < 0 {
		return config{}, fmt.Errorf("-from-offset requires an explicit -partition")
	}

	return cfg, nil
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// boundedRange is the resolved, bounded set of per-partition
// [start, end) offsets this run will read. end is exclusive and fixed at
// resolution time, so the run terminates even if the topic keeps receiving
// new records - a replay must never silently turn into a live tail.
type boundedRange struct {
	start map[int32]kgo.Offset
	end   map[int32]int64
}

func resolveRange(ctx context.Context, admin *kadm.Client, cfg config) (boundedRange, error) {
	if cfg.fromOffset >= 0 {
		return resolveOffsetRange(ctx, admin, cfg)
	}
	return resolveTimeRange(ctx, admin, cfg)
}

func resolveOffsetRange(ctx context.Context, admin *kadm.Client, cfg config) (boundedRange, error) {
	br := boundedRange{start: map[int32]kgo.Offset{}, end: map[int32]int64{}}
	br.start[cfg.partition] = kgo.NewOffset().At(cfg.fromOffset)

	if cfg.toOffset >= 0 {
		br.end[cfg.partition] = cfg.toOffset
		return br, nil
	}

	ends, err := admin.ListEndOffsets(ctx, cfg.topic)
	if err != nil {
		return boundedRange{}, fmt.Errorf("list end offsets for %s: %w", cfg.topic, err)
	}
	end, ok := ends.Lookup(cfg.topic, cfg.partition)
	if !ok {
		return boundedRange{}, fmt.Errorf("partition %d not found on topic %s", cfg.partition, cfg.topic)
	}
	br.end[cfg.partition] = end.Offset
	return br, nil
}

func resolveTimeRange(ctx context.Context, admin *kadm.Client, cfg config) (boundedRange, error) {
	fromMillis, err := parseRFC3339Millis(cfg.fromTime)
	if err != nil {
		return boundedRange{}, fmt.Errorf("-from-time: %w", err)
	}

	startOffsets, err := admin.ListOffsetsAfterMilli(ctx, fromMillis, cfg.topic)
	if err != nil {
		return boundedRange{}, fmt.Errorf("resolve start offsets for %s at %s: %w", cfg.topic, cfg.fromTime, err)
	}

	var endOffsets kadm.ListedOffsets
	if cfg.toTime != "" {
		toMillis, err := parseRFC3339Millis(cfg.toTime)
		if err != nil {
			return boundedRange{}, fmt.Errorf("-to-time: %w", err)
		}
		endOffsets, err = admin.ListOffsetsAfterMilli(ctx, toMillis, cfg.topic)
		if err != nil {
			return boundedRange{}, fmt.Errorf("resolve end offsets for %s at %s: %w", cfg.topic, cfg.toTime, err)
		}
	} else {
		endOffsets, err = admin.ListEndOffsets(ctx, cfg.topic)
		if err != nil {
			return boundedRange{}, fmt.Errorf("list end offsets for %s: %w", cfg.topic, err)
		}
	}

	br := boundedRange{start: map[int32]kgo.Offset{}, end: map[int32]int64{}}
	startOffsets.Each(func(lo kadm.ListedOffset) {
		if cfg.partition >= 0 && lo.Partition != cfg.partition {
			return
		}
		br.start[lo.Partition] = kgo.NewOffset().At(lo.Offset)
	})
	endOffsets.Each(func(lo kadm.ListedOffset) {
		if cfg.partition >= 0 && lo.Partition != cfg.partition {
			return
		}
		br.end[lo.Partition] = lo.Offset
	})
	if len(br.start) == 0 {
		return boundedRange{}, fmt.Errorf("no partitions resolved for topic %s (check -partition and topic name)", cfg.topic)
	}
	return br, nil
}

func parseRFC3339Millis(s string) (int64, error) {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return 0, err
	}
	return t.UnixMilli(), nil
}

// applier is the exact same per-record apply path the live consumer uses,
// reused unmodified so replay's idempotency comes from the handler's own
// guarantees (processed_events, ES external versioning), not a second
// mechanism built for this tool.
type applier interface {
	HandleMessage(ctx context.Context, value []byte) error
}

type dryRunApplier struct {
	describe func(value []byte) string
	logger   *slog.Logger
}

func (d dryRunApplier) HandleMessage(_ context.Context, value []byte) error {
	d.logger.Info("dry-run: would apply", "record", d.describe(value))
	return nil
}

func run(ctx context.Context, cfg config, logger *slog.Logger) error {
	kadmClient, err := kgo.NewClient(kgo.SeedBrokers(cfg.brokers...))
	if err != nil {
		return fmt.Errorf("connect kafka admin client: %w", err)
	}
	defer kadmClient.Close()
	admin := kadm.NewClient(kadmClient)

	br, err := resolveRange(ctx, admin, cfg)
	if err != nil {
		return err
	}

	seeds := map[string]map[int32]kgo.Offset{cfg.topic: br.start}
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.brokers...),
		kgo.ConsumePartitions(seeds),
	)
	if err != nil {
		return fmt.Errorf("create replay consumer: %w", err)
	}
	defer consumer.Close()

	app, closeApp, err := buildApplier(ctx, cfg, logger)
	if err != nil {
		return err
	}
	defer closeApp()

	remaining := make(map[int32]int64, len(br.end))
	maps.Copy(remaining, br.end)
	logger.Info("starting replay", "target", cfg.target, "topic", cfg.topic, "dry_run", cfg.dryRun, "partitions", len(remaining))

	var examined, applied, failed int
	for len(remaining) > 0 {
		fetches := consumer.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return err
		}
		var fetchErr error
		fetches.EachError(func(topic string, partition int32, err error) {
			fetchErr = fmt.Errorf("fetch error on %s[%d]: %w", topic, partition, err)
		})
		if fetchErr != nil {
			return fetchErr
		}

		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			end, inRange := remaining[record.Partition]
			if !inRange || record.Offset >= end {
				delete(remaining, record.Partition)
				continue
			}

			examined++
			if err := app.HandleMessage(ctx, record.Value); err != nil {
				failed++
				logger.Error("replay apply failed", "topic", record.Topic, "partition", record.Partition, "offset", record.Offset, "error", err)
			} else {
				applied++
			}

			if record.Offset+1 >= end {
				delete(remaining, record.Partition)
			}
		}
	}

	logger.Info("replay complete", "examined", examined, "applied", applied, "failed", failed, "dry_run", cfg.dryRun)
	if failed > 0 {
		return fmt.Errorf("%d of %d records failed to apply", failed, examined)
	}
	return nil
}

func buildApplier(ctx context.Context, cfg config, logger *slog.Logger) (applier, func(), error) {
	if cfg.dryRun {
		return dryRunApplier{describe: describeRecord, logger: logger}, func() {}, nil
	}

	switch cfg.target {
	case "indexer":
		esClient, err := platform.NewElasticsearchClient(cfg.elasticsearchURL)
		if err != nil {
			return nil, nil, fmt.Errorf("connect elasticsearch: %w", err)
		}
		searchClient := search.NewClient(esClient, cfg.productsIndex)
		return indexer.NewHandler(searchClient), func() {}, nil

	case "notifications":
		dsn := cfg.postgresDSN
		if dsn == "" {
			dsn = defaultPostgresDSN()
		}
		pool, err := platform.NewPostgresPool(ctx, dsn)
		if err != nil {
			return nil, nil, fmt.Errorf("connect postgres: %w", err)
		}
		return notifications.NewHandler(pool), pool.Close, nil

	default:
		return nil, nil, fmt.Errorf("unknown target %q", cfg.target)
	}
}

func defaultPostgresDSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		getenv("POSTGRES_USER", "searchsync_app"),
		os.Getenv("POSTGRES_PASSWORD"),
		getenv("POSTGRES_HOST", "localhost"),
		getenv("POSTGRES_PORT", "5432"),
		getenv("POSTGRES_DB", "searchsync"))
}

// describeRecord renders a record's raw JSON payload for dry-run output.
// Both targets' payloads are already small, non-sensitive JSON documents
// (see internal/orders/order.go's comment on OrderCreated, and the
// products row shape in internal/cdc), so printing them verbatim is safe.
func describeRecord(value []byte) string {
	if value == nil {
		return "tombstone"
	}
	return string(value)
}
