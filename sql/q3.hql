SELECT
    COALESCE(m.store, 'unknown') AS store,
    COUNT(*) AS review_count,
    ROUND(AVG(r.rating), 4) AS avg_rating,
    ROUND(AVG(CASE WHEN r.rating <= 2 THEN 1.0 ELSE 0.0 END), 4) AS low_rating_share,
    ROUND(AVG(r.helpful_vote), 4) AS avg_helpful_vote,
    ROUND(AVG(CASE WHEN r.verified_purchase THEN 1.0 ELSE 0.0 END), 4) AS verified_purchase_ratio
FROM reviews_optimized r
LEFT JOIN metadata_bucketed m
    ON r.parent_asin = m.parent_asin
GROUP BY COALESCE(m.store, 'unknown')
HAVING COUNT(*) >= 200
ORDER BY low_rating_share DESC, avg_rating ASC, review_count DESC
LIMIT 20
