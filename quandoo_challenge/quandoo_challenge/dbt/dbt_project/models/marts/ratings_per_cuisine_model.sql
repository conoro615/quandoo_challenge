/*

Another bit of analysis to calculate the average rating per cuisine 
Added the number of reviews as well just to provide extra context

*/
{{ config(materialized='table') }}
WITH final_cte AS (
SELECT
    restaurant_cuisine,
    ROUND(AVG(rating),2) AS average_rating,
    sum(reviews) AS number_of_ratings,
    COUNT(DISTINCT restaurant_id) AS number_of_restaurants
FROM
    {{ref('restaurants_model')}}
WHERE
    rating IS NOT NULL
GROUP BY
    restaurant_cuisine
ORDER BY
    number_of_restaurants DESC
)
SELECT * FROM final_cte