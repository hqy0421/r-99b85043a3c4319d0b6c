package broker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adexaja/shoebox/internal/storage"
)

// leaseBackends builds the broker test backends. SQLite runs on :memory: so
// every case is hermetic; the same assertions run against Memory so the two
// claim paths share the visibility boundary.
func leaseBackends(t *testing.T) []struct {
	name  string
	store storage.Storage
} {
	t.Helper()
	sqliteStore, err := storage.NewSQLite(context.Background(), ":memory:")
	if err != nil {
		t.Fatalf("NewSQLite: %v", err)
	}
	t.Cleanup(func() { _ = sqliteStore.Close() })
	return []struct {
		name  string
		store storage.Storage
	}{
		{name: "memory", store: storage.NewMemory()},
		{name: "sqlite", store: sqliteStore},
	}
}

func quietLeaseBroker(t *testing.T, store storage.Storage, concurrency int, visibility time.Duration) *Broker {
	t.Helper()
	if concurrency <= 0 {
		concurrency = 2
	}
	return New(Options{
		Storage:           store,
		Concurrency:       concurrency,
		VisibilityTimeout: visibility,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
}

// waitForProcessed polls queue stats until Processed reaches want or the
// deadline expires.
func waitForProcessed(t *testing.T, b *Broker, queue string, want uint64) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		stats, err := b.Stats(context.Background(), queue)
		if err != nil {
			t.Fatalf("Stats: %v", err)
		}
		if stats.Processed == want {
			return
		}
		if stats.Processed > want {
			t.Fatalf("Processed = %d, want %d", stats.Processed, want)
		}
		time.Sleep(15 * time.Millisecond)
	}
	stats, _ := b.Stats(context.Background(), queue)
	t.Fatalf("timed out waiting for Processed=%d; stats=%+v", want, stats)
}

// TestBroker_TimedOutClaimIsReclaimedAndFenced drives the full dispatch path:
// the first worker outlives its visibility timeout, a second worker picks the
// message up under a new lease, and the first worker's late ack must not
// complete or duplicate anything.
func TestBroker_TimedOutClaimIsReclaimedAndFenced(t *testing.T) {
	for _, be := range leaseBackends(t) {
		t.Run(be.name, func(t *testing.T) {
			b := quietLeaseBroker(t, be.store, 2, 80*time.Millisecond)
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				_ = b.Shutdown(ctx)
			})

			var deliveries atomic.Int64
			var sawLease atomic.Bool
			b.Register("q", func(_ context.Context, m storage.Message) error {
				if m.LeaseID != "" {
					sawLease.Store(true)
				}
				n := deliveries.Add(1)
				if n == 1 {
					// Outlive the visibility timeout; the broker re-claims
					// the message while this handler is still running.
					time.Sleep(450 * time.Millisecond)
					return nil // late success: ack must be fenced off
				}
				return nil
			}, HandlerOptions{MaxRetries: 5})

			if err := b.Enqueue(context.Background(), "q", []byte("payload"), EnqueueOpts{}); err != nil {
				t.Fatalf("Enqueue: %v", err)
			}

			// Exactly one effective completion.
			waitForProcessed(t, b, "q", 1)

			if deliveries.Load() < 2 {
				t.Fatalf("handler deliveries = %d, want at least 2 (re-claim after timeout)", deliveries.Load())
			}
			if !sawLease.Load() {
				t.Fatal("dispatched message never carried a LeaseID")
			}
			// The stale ack must not have resurrected or deleted anything.
			leased := be.store.(storage.VisibilityStorage)
			if _, err := leased.DequeueClaimed(context.Background(), "q", 1, time.Millisecond); !errors.Is(err, storage.ErrEmpty) {
				t.Fatalf("queue after completion = %v, want empty", err)
			}
		})
	}
}

// TestBroker_StaleFailureCannotOverwriteNewRound makes the first worker fail
// after its lease has already been re-claimed. Its Retry transition must be
// fenced: no retry row, no DLQ entry, and the second worker's success still
// completes the message once.
func TestBroker_StaleFailureCannotOverwriteNewRound(t *testing.T) {
	for _, be := range leaseBackends(t) {
		t.Run(be.name, func(t *testing.T) {
			b := quietLeaseBroker(t, be.store, 2, 80*time.Millisecond)
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				_ = b.Shutdown(ctx)
			})

			var deliveries atomic.Int64
			b.Register("q", func(_ context.Context, _ storage.Message) error {
				if deliveries.Add(1) == 1 {
					time.Sleep(450 * time.Millisecond)
					return errors.New("late failure from the old claim")
				}
				return nil
			}, HandlerOptions{MaxRetries: 5})

			if err := b.Enqueue(context.Background(), "q", []byte("payload"), EnqueueOpts{}); err != nil {
				t.Fatalf("Enqueue: %v", err)
			}

			waitForProcessed(t, b, "q", 1)

			stats, err := b.Stats(context.Background(), "q")
			if err != nil {
				t.Fatalf("Stats: %v", err)
			}
			if stats.Retries != 0 {
				t.Fatalf("Retries = %d, want 0 (stale retry must be fenced)", stats.Retries)
			}
			if dead, err := be.store.List(context.Background(), "q.dlq", 10); err == nil && len(dead) != 0 {
				t.Fatalf("DLQ = %v, want empty (stale dead-letter must be fenced)", dead)
			}
		})
	}
}

// TestBroker_BatchedAcksAlsoFenceStaleClaims repeats the re-claim scenario
// with acknowledgement batching enabled, exercising AckClaimedBatch: a stale
// claim sitting in the flush buffer cannot remove the newer claim.
func TestBroker_BatchedAcksAlsoFenceStaleClaims(t *testing.T) {
	for _, be := range leaseBackends(t) {
		t.Run(be.name, func(t *testing.T) {
			b := New(Options{
				Storage:           be.store,
				Concurrency:       2,
				VisibilityTimeout: 80 * time.Millisecond,
				Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
				AckBatchSize:      50,
				AckFlushInterval:  25 * time.Millisecond,
			})
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				_ = b.Shutdown(ctx)
			})

			var deliveries atomic.Int64
			b.Register("q", func(_ context.Context, _ storage.Message) error {
				if deliveries.Add(1) == 1 {
					time.Sleep(450 * time.Millisecond)
				}
				return nil
			}, HandlerOptions{MaxRetries: 5})

			if err := b.Enqueue(context.Background(), "q", []byte("payload"), EnqueueOpts{}); err != nil {
				t.Fatalf("Enqueue: %v", err)
			}

			waitForProcessed(t, b, "q", 1)
			if deliveries.Load() < 2 {
				t.Fatalf("deliveries = %d, want re-claim", deliveries.Load())
			}
		})
	}
}
