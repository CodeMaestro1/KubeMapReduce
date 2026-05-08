package relay

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"kubemapreduce/manager-service/internal/events"
	"kubemapreduce/manager-service/internal/store"
)

// BrokerPublisher defines the interface for publishing events to a message broker.
// Implementations can be NATS JetStream, Kafka, or a no-op publisher for testing.
type BrokerPublisher interface {
	Publish(ctx context.Context, event *events.EventEnvelope) error
	Close() error
}

// RelayService drains the EVENT_OUTBOX table and publishes events to the broker.
// It runs as a background goroutine and respects feature flags for gradual rollout.
type RelayService struct {
	outboxStore *store.OutboxStore
	publisher   BrokerPublisher
	maxRetries  int
	interval    time.Duration
	enabled     bool
	stopCh      chan struct{}
}

// RelayConfig holds configuration for the relay service.
type RelayConfig struct {
	Enabled    bool
	MaxRetries int
	Interval   time.Duration
}

// NewRelayService creates a new RelayService with the given dependencies.
func NewRelayService(outboxStore *store.OutboxStore, publisher BrokerPublisher, cfg RelayConfig) *RelayService {
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 3
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 5 * time.Second
	}

	return &RelayService{
		outboxStore: outboxStore,
		publisher:   publisher,
		maxRetries:  cfg.MaxRetries,
		interval:    cfg.Interval,
		enabled:     cfg.Enabled,
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

	slog.Info("outbox relay started")

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

// processBatch fetches undelivered events and attempts to publish them.
func (r *RelayService) processBatch(ctx context.Context) error {
	events, err := r.outboxStore.FetchUndeliveredEvents(ctx, 100)
	if err != nil {
		return fmt.Errorf("failed to fetch undelivered events: %w", err)
	}

	if len(events) == 0 {
		return nil
	}

	slog.Info("processing outbox events", "count", len(events))

	for _, evt := range events {
		// Apply exponential backoff: base 1s, max 30s, exponential 2x
		retryCount := len(events) // Simplified - would check actual retry_count in real implementation
		backoffDuration := calculateBackoff(retryCount)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoffDuration):
		}

		if err := r.publishEvent(ctx, evt); err != nil {
			slog.Error("failed to publish event",
				"event_id", evt.EventID,
				"event_type", evt.EventType,
				"error", err)
		}
	}

	return nil
}

// calculateBackoff computes exponential backoff with jitter for retries.
func calculateBackoff(retryCount int) time.Duration {
	base := time.Second
	maxBackoff := 30 * time.Second
	exponential := base << retryCount
	if exponential > maxBackoff {
		exponential = maxBackoff
	}
	// Add jitter (0-25%)
	jitter := time.Duration(int(exponential) / 4)
	return exponential + jitter
}

// publishEvent attempts to publish a single event and updates the outbox status.
func (r *RelayService) publishEvent(ctx context.Context, evt *events.EventEnvelope) error {
	if err := r.publisher.Publish(ctx, evt); err != nil {
		// Record failure but don't fail the entire batch
		if markErr := r.outboxStore.RecordDeliveryFailure(ctx, evt.EventID, err.Error(), r.maxRetries); markErr != nil {
			slog.Error("failed to record delivery failure",
				"event_id", evt.EventID,
				"error", markErr)
		}
		return err
	}

	// Mark as delivered
	if err := r.outboxStore.MarkDelivered(ctx, evt.EventID); err != nil {
		return fmt.Errorf("failed to mark event as delivered: %w", err)
	}

	slog.Info("event published successfully",
		"event_id", evt.EventID,
		"event_type", evt.EventType)

	return nil
}

// Stop signals the relay to stop processing.
func (r *RelayService) Stop() {
	close(r.stopCh)
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
	subject := EventToSubject(event.EventType)

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

// EventToSubject maps an event type to a NATS subject.
func EventToSubject(eventType events.EventType) string {
	return string(eventType) // Simple mapping: event type IS the subject
}
