package shoebox

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// TestQueue_VisibilityTimeoutReclaimsAndFences verifies the public
// VisibilityTimeout option end to end: a worker that outlives its claim is
// replaced by another worker under a new lease identity, the late worker's
// acknowledgement cannot overwrite the new round, and the message completes
// exactly once.
func TestQueue_VisibilityTimeoutReclaimsAndFences(t *testing.T) {
	reg := prometheus.NewRegistry()
	q, err := New(Options{
		Storage:           Memory,
		Concurrency:       2,
		VisibilityTimeout: 80 * time.Millisecond,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		MetricsRegistry:   reg,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = q.Shutdown(ctx)
	})

	var deliveries atomic.Int64
	var processed atomic.Int64
	q.Handle("q", func(_ context.Context, m Message) error {
		if m.LeaseID == "" {
			t.Error("handler received a message without a LeaseID")
		}
		if deliveries.Add(1) == 1 {
			// Outlive the visibility timeout so another worker re-claims
			// the message while this delivery is still running.
			time.Sleep(450 * time.Millisecond)
		}
		processed.Add(1)
		return nil
	}, HandlerOptions{MaxRetries: 5})

	if err := q.Enqueue("q", []byte("lease-me")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		stats, err := q.Stats(context.Background(), "q")
		if err != nil {
			t.Fatalf("Stats: %v", err)
		}
		if stats.Processed == 1 {
			break
		}
		if stats.Processed > 1 {
			t.Fatalf("Processed = %d, want exactly 1", stats.Processed)
		}
		time.Sleep(15 * time.Millisecond)
	}
	stats, err := q.Stats(context.Background(), "q")
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.Processed != 1 {
		t.Fatalf("Processed = %d, want exactly 1; deliveries=%d",
			stats.Processed, deliveries.Load())
	}
	if deliveries.Load() < 2 {
		t.Fatalf("deliveries = %d, want the timed-out claim re-delivered", deliveries.Load())
	}
}

// TestQueue_DefaultVisibilityTimeoutApplied ensures a zero VisibilityTimeout
// gets the broker default rather than disabling leases.
func TestQueue_DefaultVisibilityTimeoutApplied(t *testing.T) {
	q := newTestQueue(t)
	if q.b.Visibility() <= 0 {
		t.Fatalf("visibility = %v, want a positive default", q.b.Visibility())
	}
}
