-- 0006_visibility_leases.postgres.up.sql
-- Visibility-timeout leases. Every delivery claim stamps a random fencing
-- token (lease_id) and an absolute claim deadline (lease_expires_at). A
-- 'processing' row whose deadline has passed can be re-claimed; re-claim
-- replaces lease_id, so a late ack from the previous claimant cannot touch
-- the new claim. The deadline is persisted, so restart keeps judging expiry
-- against the original deadline instead of resetting it.
ALTER TABLE shoebox_messages ADD COLUMN IF NOT EXISTS lease_id TEXT NOT NULL DEFAULT '';
-- Nullable: NULL means no active deadline (pending/dead rows, and legacy
-- rows predating leases, which a NULL comparison treats as expired so they
-- are re-claimable immediately after upgrade).
ALTER TABLE shoebox_messages ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ;

-- Re-claim scan: due processing rows whose lease has expired, alongside the
-- pending dequeue path covered by idx_shoebox_dequeue.
CREATE INDEX IF NOT EXISTS idx_shoebox_lease_expiry
    ON shoebox_messages(queue, status, lease_expires_at);
