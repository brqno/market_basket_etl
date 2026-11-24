{{ medallion_quality_model(
    source('market_basket_bronze', 'bronze_aisles'),
    ['aisle_id', 'aisle', 'processed_data'],
    ['aisle_id', 'aisle'],
    ['aisle_id'],
    rejected=true
) }}