SELECT
    COALESCE(m.main_category, 'unknown') AS main_category,
    CASE WHEN r.verified_purchase THEN 'verified' ELSE 'not_verified' END AS purchase_type,
    COUNT(*) AS review_count,
    ROUND(AVG(r.rating), 4) AS avg_rating,
    ROUND(AVG(r.helpful_vote), 4) AS avg_helpful_vote,
    ROUND(AVG(CASE WHEN r.rating <= 2 THEN 1.0 ELSE 0.0 END), 4) AS low_rating_share
FROM reviews_optimized r
LEFT JOIN metadata_bucketed m
    ON r.parent_asin = m.parent_asin
GROUP BY
    COALESCE(m.main_category, 'unknown'),
    CASE WHEN r.verified_purchase THEN 'verified' ELSE 'not_verified' END
HAVING COUNT(*) >= 500
ORDER BY main_category, purchase_type
