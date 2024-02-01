/*

This model cleans up the menu data coming in from the scraper into the staging schema 
Also ads a previously updated row date for historical data
Can use this historical data to compare to previous record, things like price change
Model is then materialized in the data_marts schema on the DB

*/
{{ config(materialized='table') }}
WITH 
cte_1 AS (
	SELECT 
		restaurant_id,
		menu_item_name,
		CAST(REPLACE(REPLACE(menu_item_price, '€', ''),',','.')AS NUMERIC) AS menu_item_price,
		menu_item_desc,
		CAST(uploaded_at AS TIMESTAMP) AS uploaded_at,
		ROW_NUMBER() OVER (PARTITION BY restaurant_id,menu_item_name,menu_item_desc ORDER BY uploaded_at desc) AS rn, 
		LAG(uploaded_at) OVER (PARTITION BY restaurant_id,menu_item_name,menu_item_desc ORDER BY uploaded_at) AS prev_update_at
	FROM staging.menu
),

final_cte AS (
	SELECT 
		a.*,
		b.menu_item_price AS previous_price,
		a.menu_item_price - b.menu_item_price AS menu_item_price_change,
		CASE
		   WHEN a.rn = 1 THEN true
		   ELSE false
		END current_record
	FROM cte_1 a 
	LEFT JOIN cte_1 b
		ON 
			a.restaurant_id = b.restaurant_id AND
			a.menu_item_name = b.menu_item_name AND
			a.menu_item_desc = b.menu_item_desc AND
			a.rn = b.rn - 1
)

SELECT 
restaurant_id,
menu_item_name,
menu_item_desc,
menu_item_price,
menu_item_price_change,
uploaded_at,
prev_update_at
FROM final_cte 
WHERE current_record
