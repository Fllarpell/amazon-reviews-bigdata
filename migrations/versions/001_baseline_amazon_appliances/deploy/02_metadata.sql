CREATE TABLE metadata (
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
