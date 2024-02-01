/*

This model cleans up the restaurant data coming in from the scraper into the staging schema 
Also ads a previously updated row date for historical data
Can use this historical data to compare to previous record, things like rating change
Model is then materialized in the data_marts schema on the DB

*/
{{ config(materialized='table') }}
WITH cte_1 AS (
SELECT 
restaurant_id,
restaurant_name,
restaurant_area,
restaurant_cuisine,
CAST(REPLACE(rating, '/6', '') AS NUMERIC) AS rating,
CAST(REPLACE(reviews, ' reviews', '') AS numeric) AS reviews,
CAST(uploaded_at AS TIMESTAMP) AS uploaded_at,
ROW_NUMBER() OVER (PARTITION BY restaurant_id ORDER BY uploaded_at DESC) AS rn, 
LAG(uploaded_at) OVER (PARTITION BY restaurant_id ORDER BY uploaded_at) AS prev_update_at
FROM staging.restaurants
),

final_cte AS (
SELECT
a.restaurant_id,
a.restaurant_name,
a.restaurant_area,
a.restaurant_cuisine,
a.rating,
b.rating AS prev_rating,
a.rating - b.rating AS rating_change,
a.reviews,
b.reviews AS prev_reviews,
a.reviews - b.reviews AS review_change,
CASE
	WHEN a.rn = 1 THEN true
	ELSE false
END current_record,
a.uploaded_at
FROM cte_1 a 
LEFT JOIN cte_1 b
		ON 
			a.restaurant_id = b.restaurant_id AND
			a.rn = b.rn - 1
)
SELECT * FROM final_cte WHERE current_record


