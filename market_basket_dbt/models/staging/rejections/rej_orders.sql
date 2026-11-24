{{ medallion_quality_model(
    source('market_basket_bronze', 'bronze_orders'),
    ['order_id', 'user_id', 'eval_set', 'order_number', 'order_dow', 'order_hour_of_day', 'days_since_prior_order', 'processed_data'],
    ['order_id', 'user_id'],
    ['order_id'],
    rejected=true
) }}