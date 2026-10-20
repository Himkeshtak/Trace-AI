-- Safe, idempotent migration to add Kafka provenance coordinates to processed_events
-- For existing databases initialized before the provenance schema update.

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'processed_events' AND column_name = 'source_topic'
    ) THEN
        ALTER TABLE processed_events ADD COLUMN source_topic VARCHAR(120);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'processed_events' AND column_name = 'source_partition'
    ) THEN
        ALTER TABLE processed_events ADD COLUMN source_partition INTEGER 
        CHECK (source_partition IS NULL OR source_partition >= 0);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'processed_events' AND column_name = 'source_offset'
    ) THEN
        ALTER TABLE processed_events ADD COLUMN source_offset BIGINT 
        CHECK (source_offset IS NULL OR source_offset >= 0);
    END IF;
END $$;
