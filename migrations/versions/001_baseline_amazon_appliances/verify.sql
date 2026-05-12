DO $$
DECLARE
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
