/*

Analysis to get a price range for each cuisine
Also added the number of dishes per each cuisine that falls into the different categories

*/
{{ config(materialized='table') }}
WITH final_cte AS (
SELECT
    restaurant_cuisine,
	COUNT(*) as number_of_restaurants,
	ROUND(AVG(menu_item_price),2) AS average_price,
    CASE
        WHEN AVG(menu_item_price) <= 8 THEN 'Low Price'
        WHEN AVG(menu_item_price) <= 10 THEN 'Medium Price'
        WHEN AVG(menu_item_price) <= 13 THEN 'High Price'
        ELSE 'Very High Price'
    END AS price_range,
	SUM(CASE WHEN menu_item_price <= 8 THEN 1 ELSE 0 END) AS low_price_count,
    SUM(CASE WHEN menu_item_price > 8 AND menu_item_price <= 10 THEN 1 ELSE 0 END) AS medium_price_count,
    SUM(CASE WHEN menu_item_price > 10 AND menu_item_price <= 13 THEN 1 ELSE 0 END) AS high_price_count,
	SUM(CASE WHEN menu_item_price > 13 THEN 1 ELSE 0 END) AS very_high_price_count
FROM {{ref('overview_model')}}
WHERE
    menu_item_price IS NOT NULL
GROUP BY
    restaurant_cuisine
ORDER BY
    restaurant_cuisine
)
SELECT * FROM final_cte