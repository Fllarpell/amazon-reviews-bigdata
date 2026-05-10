SELECT
    COALESCE(m.main_category, 'unknown') AS main_category,
    COUNT(*) AS review_count,
    ROUND(AVG(r.rating), 4) AS avg_rating
FROM reviews_optimized r
LEFT JOIN metadata_bucketed m
    ON r.parent_asin = m.parent_asin
GROUP BY COALESCE(m.main_category, 'unknown')
ORDER BY review_count DESC
LIMIT 20
