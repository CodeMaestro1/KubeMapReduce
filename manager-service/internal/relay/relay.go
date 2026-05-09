package relay

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"kubemapreduce/manager-service/internal/events"
	"kubemapreduce/manager-service/internal/store"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// BrokerPublisher defines the interface for publishing events to a message broker.
// Implementations can be NATS JetStream, Kafka, or a no-op publisher for testing.
type BrokerPublisher interface {
	Publish(ctx context.Context, event *events.EventEnvelope) error
	Close() error
}

// MetricsHook is an optional set of callbacks the relay invokes on its hot
// path. Each field is called only if non-nil so the relay can run with no
// observability dependency in tests.
type MetricsHook struct {
	ObservePublishLatency func(eventType string, seconds float64)
	IncRetry              func(eventType string)
	IncDeadLetter         func(eventType string)
	IncPublishedTotal     func(eventType string, success bool)
}

// RelayService drains the EVENT_OUTBOX table and publishes events to the broker.
// It runs as a background goroutine and respects feature flags for gradual rollout.
type RelayService struct {
	outboxStore *store.OutboxStore
	publisher   BrokerPublisher
	maxRetries  int
	interval    time.Duration
	batchSize   int
	enabled     bool
	metrics     *MetricsHook

	stopCh   chan struct{}
	stopOnce sync.Once
}

// RelayConfig holds configuration for the relay service.
type RelayConfig struct {
	Enabled    bool
	MaxRetries int
	Interval   time.Duration
	BatchSize  int
	Metrics    *MetricsHook
}

// NewRelayService creates a new RelayService with the given dependencies.
func NewRelayService(outboxStore *store.OutboxStore, publisher BrokerPublisher, cfg RelayConfig) *RelayService {
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 3
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 5 * time.Second
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}

	return &RelayService{
		outboxStore: outboxStore,
		publisher:   publisher,
		maxRetries:  cfg.MaxRetries,
		interval:    cfg.Interval,
		batchSize:   cfg.BatchSize,
		enabled:     cfg.Enabled,
		metrics:     cfg.Metrics,
		stopCh:      make(chan struct{}),
	}
}

// Start begins the background relay loop. It returns immediately and runs the loop
// in a goroutine. The relay only processes events if the feature flag is enabled.
func (r *RelayService) Start(ctx context.Context) {
	if !r.enabled {
		slog.Info("outbox relay is disabled via feature flag")
		return
	}

	go r.run(ctx)
}

func (r *RelayService) run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	slog.Info("outbox relay started", "batch_size", r.batchSize, "interval", r.interval)

	for {
		select {
		case <-r.stopCh:
			slog.Info("outbox relay stopped")
			return
		case <-ctx.Done():
			slog.Info("outbox relay context cancelled")
			return
		case <-ticker.C:
			if err := r.processBatch(ctx); err != nil {
				slog.Error("failed to process outbox batch", "error", err)
			}
		}
	}
}

// processBatch claims a batch of due events, attempts to publish each, and
// commits success / failure status in a single transaction. There is no
// per-event sleep — backoff lives entirely in next_attempt_at, computed by
// outbox_backoff() when a failure is recorded (review M1).
func (r *RelayService) processBatch(ctx context.Context) error {
	tx, claims, err := r.outboxStore.ClaimUndeliveredEvents(ctx, r.batchSize)
	if err != nil {
		return fmt.Errorf("failed to claim outbox events: %w", err)
	}
	if len(claims) == 0 {
		// Always commit (no rows changed) to release the transaction.
		_ = tx.Rollback()
		return nil
	}

	slog.Info("processing outbox events", "count", len(claims))

	delivered := make([]uuid.UUID, 0, len(claims))
	failures := make([]store.FailureRecord, 0)

	for _, c := range claims {
		select {
		case <-ctx.Done():
			_ = tx.Rollback()
			return ctx.Err()
		default:
		}

		evt := c.Envelope
		start := time.Now()
		pubErr := r.publisher.Publish(ctx, evt)
		if r.metrics != nil && r.metrics.ObservePublishLatency != nil {
			r.metrics.ObservePublishLatency(string(evt.EventType), time.Since(start).Seconds())
		}

		if pubErr != nil {
			slog.Error("failed to publish event",
				"event_id", evt.EventID,
				"event_type", evt.EventType,
				"retry_count", c.RetryCount,
				"error", pubErr)
			failures = append(failures, store.FailureRecord{EventID: evt.EventID, Err: pubErr.Error()})
			if r.metrics != nil {
				if r.metrics.IncRetry != nil {
					r.metrics.IncRetry(string(evt.EventType))
				}
				if r.metrics.IncPublishedTotal != nil {
					r.metrics.IncPublishedTotal(string(evt.EventType), false)
				}
				if c.RetryCount+1 >= r.maxRetries && r.metrics.IncDeadLetter != nil {
					r.metrics.IncDeadLetter(string(evt.EventType))
				}
			}
			continue
		}

		delivered = append(delivered, evt.EventID)
		if r.metrics != nil && r.metrics.IncPublishedTotal != nil {
			r.metrics.IncPublishedTotal(string(evt.EventType), true)
		}
		slog.Debug("event published", "event_id", evt.EventID, "event_type", evt.EventType)
	}

	if err := r.outboxStore.MarkBatchDelivered(ctx, tx, delivered); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("mark delivered: %w", err)
	}
	if err := r.outboxStore.RecordDeliveryFailures(ctx, tx, failures, r.maxRetries); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("record failures: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit relay batch: %w", err)
	}
	return nil
}

// Stop signals the relay to stop processing. Safe to call multiple times.
func (r *RelayService) Stop() {
	r.stopOnce.Do(func() { close(r.stopCh) })
}

// NoopPublisher is a broker publisher that does nothing. Useful for testing
// or when the relay is disabled.
type NoopPublisher struct{}

// Publish is a no-op.
func (n *NoopPublisher) Publish(ctx context.Context, event *events.EventEnvelope) error {
	return nil
}

// Close is a no-op.
func (n *NoopPublisher) Close() error {
	return nil
}

// NATSPublisher publishes events to NATS JetStream.
type NATSPublisher struct {
	conn      *nats.Conn
	js        jetstream.JetStream
	url       string
	credsPath string
}

// NewNATSPublisher creates a new NATSPublisher with the given connection options.
func NewNATSPublisher(url, credsPath string) (*NATSPublisher, error) {
	opts := []nats.Option{nats.Name("KubeMapReduce Relay")}
	if credsPath != "" {
		opts = append(opts, nats.UserCredentials(credsPath))
	}

	conn, err := nats.Connect(url, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	js, err := jetstream.New(conn)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}

	return &NATSPublisher{
		conn: conn,
		js:   js,
		url:  url,
	}, nil
}

// Publish publishes an event to NATS JetStream.
func (n *NATSPublisher) Publish(ctx context.Context, event *events.EventEnvelope) error {
	subject := SubjectForEvent(event)

	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	_, err = n.js.Publish(ctx, subject, data, jetstream.WithMsgID(event.EventID.String()))
	if err != nil {
		return fmt.Errorf("failed to publish to %s: %w", subject, err)
	}

	slog.Debug("event published to NATS",
		"event_id", event.EventID,
		"event_type", event.EventType,
		"subject", subject,
	)

	return nil
}

// Close closes the NATS connection.
func (n *NATSPublisher) Close() error {
	if n.conn != nil {
		n.conn.Close()
	}
	return nil
}

// EventToSubject maps an event type to a default NATS subject. Prefer
// [SubjectForEvent] when the full envelope is available — task events are
// per-job partitioned for fan-out to subscribers (review C6).
func EventToSubject(eventType events.EventType) string {
	return string(eventType)
}

// SubjectForEvent returns the NATS subject for an event envelope. Task-
// scoped events are partitioned by job_id so workers can subscribe to
// `tasks.assigned.<job_id>` and only receive their own assignments.
func SubjectForEvent(e *events.EventEnvelope) string {
	switch e.EventType {
	case events.TaskAssigned, events.TaskIdleDetected,
		events.TaskAttemptCompleted, events.TaskAttemptFailed,
		events.TaskHeartbeatReceived, events.TaskReaped:
		if e.JobID != uuid.Nil {
			return string(e.EventType) + "." + e.JobID.String()
		}
	}
	return string(e.EventType)
}
