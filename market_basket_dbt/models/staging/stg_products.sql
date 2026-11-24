{{ medallion_quality_model(
    source('market_basket_bronze', 'bronze_products'),
    ['product_id', 'product_name', 'aisle_id', 'department_id', 'processed_data'],
    ['product_id', 'product_name'],
    ['product_id']
) }}