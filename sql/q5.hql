SELECT
    COALESCE(m.main_category, 'unknown') AS main_category,
    COUNT(*) AS review_count,
    SUM(CASE WHEN r.rating <= 2 THEN 1 ELSE 0 END) AS low_rating_count,
    ROUND(AVG(CASE WHEN r.rating <= 2 THEN 1.0 ELSE 0.0 END), 4) AS low_rating_share,
    SUM(CASE WHEN r.rating <= 2 THEN r.helpful_vote ELSE 0 END) AS low_rating_helpful_votes,
    ROUND(AVG(CASE WHEN r.rating <= 2 THEN r.helpful_vote END), 4) AS avg_helpful_on_low_ratings
FROM reviews_optimized r
LEFT JOIN metadata_bucketed m
    ON r.parent_asin = m.parent_asin
GROUP BY COALESCE(m.main_category, 'unknown')
HAVING COUNT(*) >= 500
ORDER BY low_rating_helpful_votes DESC, low_rating_share DESC, review_count DESC
LIMIT 20
