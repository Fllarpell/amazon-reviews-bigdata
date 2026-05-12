SELECT
    review_year,
    review_month,
    COUNT(*) AS review_count,
    ROUND(AVG(rating), 4) AS avg_rating,
    ROUND(AVG(CASE WHEN rating <= 2 THEN 1.0 ELSE 0.0 END), 4) AS low_rating_share,
    ROUND(AVG(CASE WHEN verified_purchase THEN 1.0 ELSE 0.0 END), 4) AS verified_purchase_ratio,
    ROUND(AVG(helpful_vote), 4) AS avg_helpful_vote
FROM reviews_optimized
GROUP BY review_year, review_month
ORDER BY review_year, review_month
