-- 0006_visibility_leases.postgres.down.sql
DROP INDEX IF EXISTS idx_shoebox_lease_expiry;
ALTER TABLE shoebox_messages DROP COLUMN IF EXISTS lease_expires_at;
ALTER TABLE shoebox_messages DROP COLUMN IF EXISTS lease_id;
