/*

Analysis to get the average menu price per restaurant

*/
{{ config(materialized='table') }}
WITH final_cte AS (
SELECT
restaurant_id,
ROUND(AVG(menu_item_price),2) AS average_price,
COUNT(*) AS number_of_menu_items
FROM
(
	SELECT * 
	FROM {{ref('menu_model')}} 
	WHERE menu_item_price IS NOT NULL)
GROUP BY 
	restaurant_id
ORDER BY 
	number_of_menu_items DESC
)

SELECT * FROM final_cte