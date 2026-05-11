SELECT
    CASE
        WHEN m.price IS NULL THEN 'unknown'
        WHEN m.price < 25 THEN '0_25'
        WHEN m.price >= 25 AND m.price < 75 THEN '25_75'
        WHEN m.price >= 75 AND m.price < 150 THEN '75_150'
        WHEN m.price >= 150 AND m.price < 300 THEN '150_300'
        ELSE '300_plus'
    END AS price_band,
    COUNT(*) AS review_count,
    ROUND(AVG(r.rating), 4) AS avg_rating,
    ROUND(AVG(CASE WHEN r.rating <= 2 THEN 1.0 ELSE 0.0 END), 4) AS low_rating_share,
    ROUND(AVG(r.helpful_vote), 4) AS avg_helpful_vote
FROM reviews_optimized r
LEFT JOIN metadata_bucketed m
    ON r.parent_asin = m.parent_asin
GROUP BY
    CASE
        WHEN m.price IS NULL THEN 'unknown'
        WHEN m.price < 25 THEN '0_25'
        WHEN m.price >= 25 AND m.price < 75 THEN '25_75'
        WHEN m.price >= 75 AND m.price < 150 THEN '75_150'
        WHEN m.price >= 150 AND m.price < 300 THEN '150_300'
        ELSE '300_plus'
    END
ORDER BY review_count DESC
