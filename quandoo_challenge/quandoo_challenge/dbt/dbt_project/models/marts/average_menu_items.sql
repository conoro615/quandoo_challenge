/*

Bit of analysis for the data marts to get the average number of items per cuisine 
Seeing if particular cuisines offer a larger menu than others
Also added number of restaurants to provide context (e.g. a unique cuisine offering a large menu that would skew perception)

*/
{{ config(materialized='table') }}
WITH count_items AS (
SELECT
	restaurant_id,
	restaurant_cuisine,
	COUNT(menu_item_name) AS number_of_menu_items
FROM {{ref('overview_model')}}
GROUP BY 
	restaurant_id,restaurant_cuisine
),

final_cte AS (	
SELECT 
	restaurant_cuisine,
	ROUND(AVG(number_of_menu_items)) AS avg_items,
	COUNT(*) AS number_of_restaurants
FROM count_items
GROUP BY 
	restaurant_cuisine
ORDER BY 
	avg_items DESC
)
	
SELECT * FROM final_cte