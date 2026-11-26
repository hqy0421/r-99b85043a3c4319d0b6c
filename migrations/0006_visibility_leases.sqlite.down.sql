-- 0006_visibility_leases.sqlite.down.sql
DROP INDEX IF EXISTS idx_shoebox_lease_expiry;
ALTER TABLE shoebox_messages DROP COLUMN lease_expires_at;
ALTER TABLE shoebox_messages DROP COLUMN lease_id;
