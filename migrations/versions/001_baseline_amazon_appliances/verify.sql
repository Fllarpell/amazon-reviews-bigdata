DO $$
DECLARE
    dist_count integer;
    fk_count integer;
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'metadata'
    ) THEN
        RAISE EXCEPTION 'verify 001: table public.metadata is missing';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'reviews'
    ) THEN
        RAISE EXCEPTION 'verify 001: table public.reviews is missing';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'citus') THEN
        RAISE EXCEPTION 'verify 001: citus extension is missing';
    END IF;
    SELECT COUNT(*) INTO dist_count
    FROM pg_dist_partition
    WHERE logicalrelid IN ('public.metadata'::regclass, 'public.reviews'::regclass);
    IF dist_count <> 2 THEN
        RAISE EXCEPTION 'verify 001: expected 2 citus distributed tables, got %', dist_count;
    END IF;
    SELECT COUNT(*) INTO fk_count
    FROM information_schema.table_constraints
    WHERE constraint_schema = 'public'
      AND table_name = 'reviews'
      AND constraint_name = 'fk_reviews_metadata'
      AND constraint_type = 'FOREIGN KEY';
    IF fk_count <> 1 THEN
        RAISE EXCEPTION 'verify 001: fk_reviews_metadata missing';
    END IF;
END $$;
