{{ medallion_quality_model(
    source('market_basket_bronze', 'bronze_order_products_prior'),
    ['order_id', 'product_id', 'add_to_cart_order', 'reordered', 'processed_data'],
    ['order_id', 'product_id', 'add_to_cart_order', 'reordered'],
    ['order_id', 'product_id']
) }}