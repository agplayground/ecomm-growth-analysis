-- Aggregating reached stages by session and traffic source
WITH session_funnel AS (
  SELECT
    session_id,
    traffic_source,
    MAX(CASE WHEN event_type = 'product'  THEN 1 ELSE 0 END) AS reached_product,
    MAX(CASE WHEN event_type = 'cart'     THEN 1 ELSE 0 END) AS reached_cart,
    MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS reached_purchase,
    MAX(CASE WHEN event_type = 'cancel'   THEN 1 ELSE 0 END) AS reached_cancel
  FROM `bigquery-public-data.thelook_ecommerce.events`
  GROUP BY 1,2
)
-- Calculating funnel metrics by traffic source, for the sake of this analysis the funnel is built from product onwards
SELECT
  traffic_source,
  SUM(reached_product)                                                                  AS product_sessions,
  SUM(reached_cart)                                                                     AS cart_sessions,
  ROUND(SAFE_DIVIDE((SUM(reached_product) - SUM(reached_cart)),SUM(reached_product)),2) AS cart_drop_off,
  SUM(reached_purchase)                                                                 AS purchase_sessions,
  ROUND(SAFE_DIVIDE((SUM(reached_cart) - SUM(reached_purchase)),SUM(reached_cart)),2)   AS purchase_drop_off,
  SUM(reached_cancel)                                                                   AS cancel_sessions,
  ROUND(SAFE_DIVIDE(SUM(reached_cancel),SUM(reached_purchase)),2)                       AS cancel_rate
FROM session_funnel
WHERE 
  reached_product = 1
GROUP BY 1
