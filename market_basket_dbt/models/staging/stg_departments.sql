{{ medallion_quality_model(
    source('market_basket_bronze', 'bronze_departments'),
    ['department_id', 'department', 'processed_data'],
    ['department_id', 'department'],
    ['department_id']
) }}