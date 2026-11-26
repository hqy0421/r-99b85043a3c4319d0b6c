// Package storage defines the Storage interface that abstracts over the
// queue's persistence layer. The broker only ever talks to Storage; the
// concrete backend (Memory, SQLite, Postgres) is selected at New() time.
package storage

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strconv"
	"time"
)

// Message is the storage-layer view of a queued message. It mirrors the
// public shoebox.Message so callers don't need to translate between the
// two; the broker is the only package that does so.
type Message struct {
	ID          string
	Queue       string
	Payload     []byte
	Attempts    int
	MaxRetries  int
	CreatedAt   time.Time
	ScheduledAt time.Time
	DedupeKey   string
	Metadata    map[string]string

	// Priority is the delivery hint. Higher values are dequeued first
	// (priority DESC) within the same due window; ties break by
	// created_at ASC. Zero (the default) is the lowest priority.
	Priority int

	// Error is set when the message is moved to the dead-letter queue; it
	// holds the last handler error. Empty for live messages.
	Error string

	// DeadAt is set when the message is moved to the dead-letter queue; it
	// records the time of dead-lettering. Zero for live messages.
	DeadAt time.Time

	// LeaseID is the fencing token of the current delivery claim. It is
	// assigned on every Dequeue — including every re-claim of a message
	// whose previous claim timed out — and is empty for a pending message
	// that has never been claimed. Ack, Retry, and DeadLetter only affect a
	// row whose LeaseID matches the claimant's, so a late confirmation from
	// an earlier claim can never overwrite a newer claim or a completed
	// message.
	LeaseID string

	// LeaseExpiresAt is the absolute deadline of the current delivery claim.
	// A 'processing' message whose LeaseExpiresAt is in the past is eligible
	// for re-claim by another worker without any restart. The deadline is
	// stored, not computed from a timeout duration, so a restart keeps
	// judging expiry against the original deadline.
	LeaseExpiresAt time.Time
}

// Schedule is a persistent periodic enqueue definition.
type Schedule struct {
	ID             string
	Queue          string
	Payload        []byte
	EnqueueOptions []byte
	Interval       time.Duration

	NextRunAt time.Time
	Enabled   bool
	CreatedAt time.Time
	UpdatedAt time.Time
}

func nonNegativeUint64(v int64) uint64 {
	if v < 0 {
		return 0
	}
	return uint64(v)
}

func deadLetterMessage(queue string, msg Message, err error, now time.Time) Message {
	msg.Queue = queue + ".dlq"
	msg.ID = NewMessageID()
	msg.ScheduledAt = now
	msg.DeadAt = now
	msg.Error = ""
	if err != nil {
		msg.Error = err.Error()
	}
	// A DLQ row is terminal, not a claim: clear any lease so a later
	// Replay starts from an unclaimed pending message.
	msg.LeaseID = ""
	msg.LeaseExpiresAt = time.Time{}
	return msg
}

func replayMessage(queue string, msg Message, now time.Time) Message {
	msg.Queue = queue
	msg.ScheduledAt = now
	msg.Error = ""
	msg.DeadAt = time.Time{}
	// Replay hands the message out as a fresh pending message; its next
	// Dequeue assigns a new lease.
	msg.LeaseID = ""
	msg.LeaseExpiresAt = time.Time{}
	return msg
}

func periodicMessage(schedule Schedule, now time.Time) Message {
	return Message{
		ID:          NewMessageID(),
		Queue:       schedule.Queue,
		Payload:     append([]byte(nil), schedule.Payload...),
		CreatedAt:   now,
		ScheduledAt: now,
	}
}

// ScheduleStore persists periodic enqueue definitions and executes one
// occurrence atomically with its cadence advance.
type ScheduleStore interface {
	CreateSchedule(ctx context.Context, schedule Schedule) error
	UpdateSchedule(ctx context.Context, schedule Schedule) error
	DeleteSchedule(ctx context.Context, id string) error
	ListSchedules(ctx context.Context, queue string) ([]Schedule, error)
	RunSchedule(ctx context.Context, schedule Schedule, now, next time.Time) (bool, error)
	DueSchedules(ctx context.Context, now time.Time, limit int) ([]Schedule, error)
}

var ErrScheduleExists = errors.New("shoebox/storage: schedule already exists")

// QueueStats is the storage-layer view of queue statistics.
type QueueStats struct {
	Queue     string
	Depth     int
	Processed uint64
	Errors    uint64
	Retries   uint64
	Dead      uint64
}

// ErrEmpty is returned by Dequeue when no messages are available.
var ErrEmpty = errors.New("shoebox/storage: queue empty")

// ErrLeaseLost is returned by lease-aware Ack, Retry, and DeadLetter calls
// when the caller's claim is no longer current: the message was re-claimed
// after the lease expired, or it was already completed by another claim. It
// is a normal at-least-once outcome, not a storage failure — the broker logs
// it as a stale-claim skip rather than treating it as an error.
var ErrLeaseLost = errors.New("shoebox/storage: lease no longer held")

// Claim pairs a message ID with the LeaseID of the delivery claim that is
// acting on it. AckBatch takes claims so each acknowledgement is fenced
// individually.
type Claim struct {
	MessageID string
	LeaseID   string
}

// VisibilityStorage is the lease-aware extension of Storage. Every built-in
// backend implements it; callers that need crash-safe hand-off (the broker,
// the pull API) type-assert to it and fall back to the plain methods when a
// backend does not provide leases.
//
// DequeueClaimed behaves like Dequeue but stamps every returned message with
// a fresh LeaseID and a LeaseExpiresAt of now+visibilityTimeout. Pending
// messages and processing messages whose lease has expired are both eligible;
// an expired processing message keeps its identity and payload but receives a
// new LeaseID, so a late ack from the previous claimant cannot touch the new
// claim.
//
// The lease-aware Ack, Retry, and DeadLetter match both the message ID and
// the claimant's LeaseID. A mismatched or completed claim returns ErrLeaseLost.
type VisibilityStorage interface {
	Storage
	DequeueClaimed(ctx context.Context, queue string, limit int, visibilityTimeout time.Duration) ([]Message, error)
	AckClaimed(ctx context.Context, queue, msgID, leaseID string) error
	AckClaimedBatch(ctx context.Context, queue string, claims []Claim) error
	RetryClaimed(ctx context.Context, queue string, m Message, err error) error
	DeadLetterClaimed(ctx context.Context, queue string, m Message, err error) error
}

// Storage is the interface every backend implements.
//
// Enqueue persists a new message. EnqueueBatch persists multiple messages in
// one committed operation. Dequeue returns up to `limit` messages that are due
// (ScheduledAt <= now), atomically transitioning them to an in-flight state
// (SQLite/Postgres: status='processing'; Memory: removed from the pending
// slice). Ack and AckBatch confirm successful processing and remove messages.
//
// Retry atomically records a failed delivery and persists the updated
// message for a future delivery. DeadLetter atomically moves a failed
// message to its queue's DLQ and records the terminal outcome. Replay
// atomically moves a DLQ message back to its source queue. List returns
// dead messages from a queue. Reclaim transitions stale in-flight messages
// back to pending (crash recovery; called once during open of a persistent
// backend).
type Storage interface {
	Enqueue(ctx context.Context, queue string, m Message) error
	EnqueueBatch(ctx context.Context, queue string, messages []Message) error
	Dequeue(ctx context.Context, queue string, limit int) ([]Message, error)
	Ack(ctx context.Context, queue string, msgID string) error
	AckBatch(ctx context.Context, queue string, msgIDs []string) error
	Retry(ctx context.Context, queue string, m Message, err error) error
	DeadLetter(ctx context.Context, queue string, m Message, err error) error
	Replay(ctx context.Context, queue string, msgID string) error
	Stats(ctx context.Context, queue string) (QueueStats, error)
	List(ctx context.Context, queue string, limit int) ([]Message, error)
	Reclaim(ctx context.Context, queue string) error
	Close() error
}

// NewMessageID returns a 16-byte random hex string suitable for use as a
// message ID. We avoid pulling in a UUID library to keep the dependency
// surface small (see ADR 0004). If crypto/rand fails (unrecoverable on a
// healthy system) it falls back to a time-based identifier so we never
// panic in a user's hot path.
func NewMessageID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		now := time.Now().UnixNano()
		return "ts-" + hex.EncodeToString([]byte(strconv.FormatInt(now, 10)))
	}
	return hex.EncodeToString(b[:])
}

// NewLeaseID returns a fresh random fencing token for one delivery claim.
// Re-claiming a timed-out message always produces a different token from the
// previous claim, which is what makes stale acks unable to overwrite the new
// claim. It uses the same generator as NewMessageID.
func NewLeaseID() string {
	return NewMessageID()
}
