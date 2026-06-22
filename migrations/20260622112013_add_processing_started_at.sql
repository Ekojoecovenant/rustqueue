-- Add migration script here
ALTER TABLE jobs ADD COLUMN processing_started_at TIMESTAMPTZ;