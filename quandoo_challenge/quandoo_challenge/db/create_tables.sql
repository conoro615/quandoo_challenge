-- Create the schema for the data to come in
CREATE SCHEMA staging;
-- Create a stageing table named "restaurants" to initially hold restaurant data
CREATE TABLE staging.restaurants (
    restaurant_id VARCHAR(255),
    restaurant_name VARCHAR(255),
    restaurant_area VARCHAR(255),
    restaurant_cuisine VARCHAR(255),
    rating VARCHAR(255),
    reviews VARCHAR(255),
    uploaded_at VARCHAR(255)
);

-- Create a stageing table named "menu" to initially hold menu data
CREATE TABLE staging.menu (
    restaurant_id VARCHAR(255),
    menu_item_name VARCHAR(255),
    menu_item_desc VARCHAR,
    menu_item_price VARCHAR(255),
    uploaded_at VARCHAR(255)
);