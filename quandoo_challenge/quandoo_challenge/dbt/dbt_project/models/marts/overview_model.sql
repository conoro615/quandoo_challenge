/*

Overview table that combines the restautrant data tot the menu data of use later
Some of my menu calcualtions require restaurant information so best to join them once here and infer dependency with DBT
so that this will build first

*/
{{ config(materialized='table') }}
WITH final_cte AS (
SELECT
rest.restaurant_id,
rest.restaurant_name,
rest.restaurant_area,
rest.restaurant_cuisine,
rest.rating,
rest.prev_rating,
rest.rating_change,
rest.reviews,
rest.review_change,
menu.menu_item_name,
menu.menu_item_desc,
menu.menu_item_price,
menu.menu_item_price_change,
rest.uploaded_at AS restaurant_data_uploaded_at,
menu.uploaded_at AS menu_data_uploaded_at
FROM {{ref('restaurants_model')}} rest
LEFT JOIN {{ref('menu_model')}} menu
ON
menu.restaurant_id = rest.restaurant_id)

SELECT 
*
FROM final_cte