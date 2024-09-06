SELECT brand,
       COUNT(*) AS brand_num
FROM {{ ref('fact_table') }} AS t
GROUP BY brand
ORDER BY brand_num DESC
LIMIT 5
