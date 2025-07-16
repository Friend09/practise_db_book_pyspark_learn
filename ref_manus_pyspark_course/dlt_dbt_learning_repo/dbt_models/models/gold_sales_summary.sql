-- models/gold_sales_summary.sql

{{ config(materialized='table') }}

SELECT
  product_name,
  city,
  SUM(total_price) AS total_revenue
FROM
  {{ source('silver', 'sales') }} -- Assuming your Silver layer table is named 'sales' in the 'silver' schema/database
GROUP BY
  product_name,
  city
ORDER BY
  total_revenue DESC


