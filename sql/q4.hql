SELECT
    COALESCE(m.main_category, 'unknown') AS main_category,
    COUNT(*) AS review_count,
    ROUND(AVG(CASE WHEN r.verified_purchase THEN 1.0 ELSE 0.0 END), 4) AS verified_purchase_ratio,
    ROUND(AVG(r.helpful_vote), 4) AS avg_helpful_vote
FROM reviews_optimized r
LEFT JOIN metadata_bucketed m
    ON r.parent_asin = m.parent_asin
GROUP BY COALESCE(m.main_category, 'unknown')
HAVING COUNT(*) >= 100
ORDER BY verified_purchase_ratio DESC, review_count DESC
LIMIT 20
