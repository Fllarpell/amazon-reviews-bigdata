ALTER TABLE reviews
    ADD CONSTRAINT fk_reviews_metadata
    FOREIGN KEY (parent_asin) REFERENCES metadata (parent_asin);
