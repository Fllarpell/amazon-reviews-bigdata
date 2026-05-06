CREATE TABLE reviews (
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
