package storage

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// leaseBackends is the single set of backends the visibility-lease
// semantics are verified against. Memory and SQLite run everywhere; Postgres
// tests skip themselves when no server is reachable. Running the same
// assertions against all three is what keeps the in-memory and persistent
// claim-expiry boundaries identical.
func leaseBackends(t *testing.T) []struct {
	name       string
	store      VisibilityStorage
	persistent bool
	path       string // SQLite file path, empty otherwise
} {
	t.Helper()
	out := []struct {
		name       string
		store      VisibilityStorage
		persistent bool
		path       string
	}{
		{name: "memory", store: NewMemory()},
		{name: "sqlite", store: newTestSQLite(t), persistent: true},
	}
	if pg, err := NewPostgres(context.Background(), testDSN); err == nil {
		_ = pg.Close()
		out = append(out, struct {
			name       string
			store      VisibilityStorage
			persistent bool
			path       string
		}{name: "postgres", store: newTestPostgres(t), persistent: true})
	}
	return out
}

// TestLease_ReclaimAfterExpiryIssuesNewIdentity covers:
//   - a claimed message is not re-delivered while its lease is valid;
//   - once the claim deadline passes it can be claimed again;
//   - the re-claim keeps the message identity but mints a different
//     LeaseID, so the two delivery rounds are distinguishable.
func TestLease_ReclaimAfterExpiryIssuesNewIdentity(t *testing.T) {
	for _, be := range leaseBackends(t) {
		t.Run(be.name, func(t *testing.T) {
			ctx := context.Background()
			if err := be.store.Enqueue(ctx, "q", Message{ID: "m1", Payload: []byte("p")}); err != nil {
				t.Fatalf("Enqueue: %v", err)
			}

			first, err := be.store.DequeueClaimed(ctx, "q", 1, 80*time.Millisecond)
			if err != nil {
				t.Fatalf("first DequeueClaimed: %v", err)
			}
			if len(first) != 1 || first[0].ID != "m1" || first[0].LeaseID == "" {
				t.Fatalf("first claim = %v", first)
			}
			if first[0].LeaseExpiresAt.IsZero() {
				t.Fatal("LeaseExpiresAt not stamped")
			}

			// Within the window: the claim is exclusive.
			if _, err := be.store.DequeueClaimed(ctx, "q", 1, 80*time.Millisecond); !errors.Is(err, ErrEmpty) {
				t.Fatalf("dequeue inside lease window = %v, want ErrEmpty", err)
			}

			time.Sleep(100 * time.Millisecond)

			second, err := be.store.DequeueClaimed(ctx, "q", 1, time.Minute)
			if err != nil {
				t.Fatalf("second DequeueClaimed: %v", err)
			}
			if len(second) != 1 || second[0].ID != "m1" {
				t.Fatalf("re-claim = %v, want the same message m1", second)
			}
			if second[0].LeaseID == first[0].LeaseID {
				t.Fatalf("re-claim LeaseID = %q, want a token different from %q",
					second[0].LeaseID, first[0].LeaseID)
			}
		})
	}
}

// TestLease_StaleAckCannotOverwriteNewRound covers the fencing guarantee:
// the previous claimant's late ack is rejected, the newer claim keeps
// running, and the currently valid ack completes the message exactly once.
func TestLease_StaleAckCannotOverwriteNewRound(t *testing.T) {
	for _, be := range leaseBackends(t) {
		t.Run(be.name, func(t *testing.T) {
			ctx := context.Background()
			if err := be.store.Enqueue(ctx, "q", Message{ID: "m1", Payload: []byte("p")}); err != nil {
				t.Fatalf("Enqueue: %v", err)
			}

			first, err := be.store.DequeueClaimed(ctx, "q", 1, 60*time.Millisecond)
			if err != nil {
				t.Fatalf("first DequeueClaimed: %v", err)
			}
			time.Sleep(80 * time.Millisecond)
			second, err := be.store.DequeueClaimed(ctx, "q", 1, time.Minute)
			if err != nil {
				t.Fatalf("second DequeueClaimed: %v", err)
			}

			// Old round's late ack must not touch the new claim.
			if err := be.store.AckClaimed(ctx, "q", "m1", first[0].LeaseID); !errors.Is(err, ErrLeaseLost) {
				t.Fatalf("stale AckClaimed = %v, want ErrLeaseLost", err)
			}

			// New round's ack completes once.
			if err := be.store.AckClaimed(ctx, "q", "m1", second[0].LeaseID); err != nil {
				t.Fatalf("current AckClaimed: %v", err)
			}
			if err := be.store.AckClaimed(ctx, "q", "m1", second[0].LeaseID); !errors.Is(err, ErrLeaseLost) {
				t.Fatalf("second AckClaimed = %v, want ErrLeaseLost (already completed)", err)
			}
			if _, err := be.store.DequeueClaimed(ctx, "q", 1, time.Millisecond); !errors.Is(err, ErrEmpty) {
				t.Fatalf("dequeue after ack = %v, want ErrEmpty", err)
			}

			stats, err := be.store.Stats(ctx, "q")
			if err != nil {
				t.Fatalf("Stats: %v", err)
			}
			if stats.Processed != 1 {
				t.Fatalf("Processed = %d, want exactly 1", stats.Processed)
			}
		})
	}
}

// TestLease_StaleRetryAndDeadLetterAreFenced verifies the other two terminal
// operations carry the same fence: a late failure from an expired claim can
// neither re-enqueue nor dead-letter a message that another worker now owns.
func TestLease_StaleRetryAndDeadLetterAreFenced(t *testing.T) {
	for _, be := range leaseBackends(t) {
		t.Run(be.name, func(t *testing.T) {
			ctx := context.Background()
			for _, scenario := range []string{"retry", "deadletter"} {
				if err := be.store.Enqueue(ctx, "q", Message{ID: "m-" + scenario, Payload: []byte("p")}); err != nil {
					t.Fatalf("Enqueue: %v", err)
				}
				first, err := be.store.DequeueClaimed(ctx, "q", 1, 60*time.Millisecond)
				if err != nil {
					t.Fatalf("first DequeueClaimed: %v", err)
				}
				time.Sleep(80 * time.Millisecond)
				second, err := be.store.DequeueClaimed(ctx, "q", 1, time.Minute)
				if err != nil {
					t.Fatalf("second DequeueClaimed: %v", err)
				}
				stale := first[0]
				switch scenario {
				case "retry":
					if err := be.store.RetryClaimed(ctx, "q", stale, errors.New("late failure")); !errors.Is(err, ErrLeaseLost) {
						t.Fatalf("stale RetryClaimed = %v, want ErrLeaseLost", err)
					}
				case "deadletter":
					if err := be.store.DeadLetterClaimed(ctx, "q", stale, errors.New("late failure")); !errors.Is(err, ErrLeaseLost) {
						t.Fatalf("stale DeadLetterClaimed = %v, want ErrLeaseLost", err)
					}
				}

				// The new claim still owns the message and can ack it.
				if err := be.store.AckClaimed(ctx, "q", second[0].ID, second[0].LeaseID); err != nil {
					t.Fatalf("current AckClaimed after stale %s: %v", scenario, err)
				}
				if dead, err := be.store.List(ctx, "q.dlq", 10); err == nil && len(dead) != 0 {
					t.Fatalf("stale %s wrote a DLQ record: %v", scenario, dead)
				}
			}
		})
	}
}

// TestLease_BatchAckFencesEachClaim checks that one batch can contain both
// stale and current claims: stale entries are skipped, current entries
// removed, and processed counts only the removals.
func TestLease_BatchAckFencesEachClaim(t *testing.T) {
	for _, be := range leaseBackends(t) {
		t.Run(be.name, func(t *testing.T) {
			ctx := context.Background()
			for _, id := range []string{"a", "b", "c"} {
				if err := be.store.Enqueue(ctx, "q", Message{ID: id, Payload: []byte(id)}); err != nil {
					t.Fatalf("Enqueue: %v", err)
				}
			}

			first, err := be.store.DequeueClaimed(ctx, "q", 3, 60*time.Millisecond)
			if err != nil || len(first) != 3 {
				t.Fatalf("first DequeueClaimed: %v %v", first, err)
			}
			time.Sleep(80 * time.Millisecond)
			second, err := be.store.DequeueClaimed(ctx, "q", 3, time.Minute)
			if err != nil || len(second) != 3 {
				t.Fatalf("second DequeueClaimed: %v %v", second, err)
			}
			byID := map[string]string{}
			for _, m := range second {
				byID[m.ID] = m.LeaseID
			}

			err = be.store.AckClaimedBatch(ctx, "q", []Claim{
				{MessageID: "a", LeaseID: byID["a"]},               // current
				{MessageID: "b", LeaseID: first[1].LeaseID},        // stale
				{MessageID: "c", LeaseID: "totally-unknown-token"}, // stale
				{MessageID: "gone", LeaseID: "never-existed"},      // unknown id
			})
			if err != nil {
				t.Fatalf("AckClaimedBatch: %v", err)
			}

			stats, err := be.store.Stats(ctx, "q")
			if err != nil {
				t.Fatalf("Stats: %v", err)
			}
			if stats.Processed != 1 {
				t.Fatalf("Processed = %d, want 1 (only the current claim)", stats.Processed)
			}
			// b and c remain owned by their new lease.
			if err := be.store.AckClaimed(ctx, "q", "b", byID["b"]); err != nil {
				t.Fatalf("ack b with current lease: %v", err)
			}
			if err := be.store.AckClaimed(ctx, "q", "c", byID["c"]); err != nil {
				t.Fatalf("ack c with current lease: %v", err)
			}
		})
	}
}

// TestLease_RestartKeepsOriginalDeadline verifies the persisted deadline:
// a claim that was still valid when the process exited stays non-deliverable
// after reopen until the original deadline passes, and the re-claim then
// mints a new identity. Memory is volatile and has no restart, so it is
// excluded.
func TestLease_RestartKeepsOriginalDeadline(t *testing.T) {
	ctx := context.Background()

	t.Run("sqlite", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "restart.db")
		s1, err := NewSQLite(ctx, path)
		if err != nil {
			t.Fatalf("NewSQLite s1: %v", err)
		}
		if err := s1.Enqueue(ctx, "q", Message{ID: "survivor", Payload: []byte("x")}); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		first, err := s1.DequeueClaimed(ctx, "q", 1, 1200*time.Millisecond)
		if err != nil {
			t.Fatalf("DequeueClaimed s1: %v", err)
		}
		_ = s1.Close()

		s2, err := NewSQLite(ctx, path)
		if err != nil {
			t.Fatalf("NewSQLite s2: %v", err)
		}
		defer func() { _ = s2.Close() }()

		// Immediately after reopen the original deadline still holds.
		if _, err := s2.DequeueClaimed(ctx, "q", 1, time.Minute); !errors.Is(err, ErrEmpty) {
			t.Fatalf("dequeue right after restart = %v, want ErrEmpty", err)
		}

		time.Sleep(1250 * time.Millisecond)
		second, err := s2.DequeueClaimed(ctx, "q", 1, time.Minute)
		if err != nil {
			t.Fatalf("DequeueClaimed after deadline: %v", err)
		}
		if len(second) != 1 || second[0].ID != "survivor" {
			t.Fatalf("re-claim = %v, want survivor", second)
		}
		if second[0].LeaseID == first[0].LeaseID {
			t.Fatal("re-claim after restart reused the old LeaseID")
		}
	})

	t.Run("postgres", func(t *testing.T) {
		skipIfNoPostgres(t)
		s1 := newTestPostgres(t)
		if err := s1.Enqueue(ctx, "q", Message{ID: "pg-survivor", Payload: []byte("x")}); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		first, err := s1.DequeueClaimed(ctx, "q", 1, 1200*time.Millisecond)
		if err != nil {
			t.Fatalf("DequeueClaimed s1: %v", err)
		}
		_ = s1.Close()

		s2, err := NewPostgres(ctx, testDSN)
		if err != nil {
			t.Fatalf("NewPostgres s2: %v", err)
		}
		defer func() { _ = s2.Close() }()

		if _, err := s2.DequeueClaimed(ctx, "q", 1, time.Minute); !errors.Is(err, ErrEmpty) {
			t.Fatalf("dequeue right after restart = %v, want ErrEmpty", err)
		}

		time.Sleep(1250 * time.Millisecond)
		second, err := s2.DequeueClaimed(ctx, "q", 1, time.Minute)
		if err != nil {
			t.Fatalf("DequeueClaimed after deadline: %v", err)
		}
		if len(second) != 1 || second[0].ID != "pg-survivor" {
			t.Fatalf("re-claim = %v, want pg-survivor", second)
		}
		if second[0].LeaseID == first[0].LeaseID {
			t.Fatal("re-claim after restart reused the old LeaseID")
		}
	})
}

// TestLease_RetryReenqueueResetsClaim verifies a retried message goes back
// to pending without a lease, so its next delivery starts a brand-new claim
// rather than inheriting the failed one.
func TestLease_RetryReenqueueResetsClaim(t *testing.T) {
	for _, be := range leaseBackends(t) {
		t.Run(be.name, func(t *testing.T) {
			ctx := context.Background()
			if err := be.store.Enqueue(ctx, "q", Message{ID: "m1", Payload: []byte("p")}); err != nil {
				t.Fatalf("Enqueue: %v", err)
			}
			first, err := be.store.DequeueClaimed(ctx, "q", 1, time.Minute)
			if err != nil {
				t.Fatalf("DequeueClaimed: %v", err)
			}
			msg := first[0]
			msg.Attempts = 1
			if err := be.store.RetryClaimed(ctx, "q", msg, errors.New("nope")); err != nil {
				t.Fatalf("RetryClaimed: %v", err)
			}

			again, err := be.store.DequeueClaimed(ctx, "q", 1, time.Minute)
			if err != nil {
				t.Fatalf("re-delivery: %v", err)
			}
			if len(again) != 1 || again[0].ID != "m1" {
				t.Fatalf("re-delivered = %v", again)
			}
			if again[0].LeaseID == "" || again[0].LeaseID == first[0].LeaseID {
				t.Fatalf("re-delivery lease = %q, want a fresh token", again[0].LeaseID)
			}
			if again[0].Attempts != 1 {
				t.Fatalf("attempts = %d, want 1 preserved", again[0].Attempts)
			}
		})
	}
}
