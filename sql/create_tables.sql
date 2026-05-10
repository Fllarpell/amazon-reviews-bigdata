START TRANSACTION;

DROP TABLE IF EXISTS reviews CASCADE;
DROP TABLE IF EXISTS metadata CASCADE;

CREATE TABLE IF NOT EXISTS metadata (
    parent_asin VARCHAR(32) PRIMARY KEY,
    main_category TEXT,
    title TEXT,
    average_rating DOUBLE PRECISION,
    rating_number INTEGER,
    price NUMERIC(12, 2),
    store TEXT,
    features_text TEXT,
    description_text TEXT,
    categories_json TEXT,
    details_json TEXT
);

CREATE TABLE IF NOT EXISTS reviews (
    review_id VARCHAR(64) NOT NULL,
    parent_asin VARCHAR(32) NOT NULL,
    user_id VARCHAR(128) NOT NULL,
    asin VARCHAR(32),
    rating INTEGER NOT NULL,
    review_title TEXT,
    review_text TEXT,
    review_timestamp TIMESTAMPTZ NOT NULL,
    helpful_vote INTEGER,
    verified_purchase BOOLEAN,
    images_json TEXT,
    PRIMARY KEY (parent_asin, review_id)
);

ALTER TABLE reviews DROP CONSTRAINT IF EXISTS fk_reviews_metadata;
ALTER TABLE reviews
    ADD CONSTRAINT fk_reviews_metadata
    FOREIGN KEY (parent_asin) REFERENCES metadata (parent_asin);

COMMIT;
