COPY metadata (
    parent_asin,
    main_category,
    title,
    average_rating,
    rating_number,
    price,
    store,
    features_text,
    description_text,
    categories_json,
    details_json
) FROM STDIN WITH (FORMAT CSV, HEADER true, NULL '');

COPY reviews (
    review_id,
    parent_asin,
    user_id,
    asin,
    rating,
    review_title,
    review_text,
    review_timestamp,
    helpful_vote,
    verified_purchase,
    images_json
) FROM STDIN WITH (FORMAT CSV, HEADER true, NULL '');
