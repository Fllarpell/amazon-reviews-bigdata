SELECT
    COALESCE(m.store, 'unknown') AS store,
    COUNT(*) AS review_count,
    ROUND(AVG(r.rating), 4) AS avg_rating,
    ROUND(AVG(r.helpful_vote), 4) AS avg_helpful_vote
FROM reviews_optimized r
LEFT JOIN metadata_bucketed m
    ON r.parent_asin = m.parent_asin
GROUP BY COALESCE(m.store, 'unknown')
HAVING COUNT(*) >= 50
ORDER BY avg_rating DESC, review_count DESC
LIMIT 20
