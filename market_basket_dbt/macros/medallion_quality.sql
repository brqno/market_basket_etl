{% macro pascal_case_column(column) -%}
{% set aliases = {
    'order_id': 'Order_ID',
    'user_id': 'User_ID',
    'eval_set': 'Eval_Set',
    'order_number': 'Order_Number',
    'order_dow': 'Order_DOW',
    'order_hour_of_day': 'Order_Hour_Of_Day',
    'days_since_prior_order': 'Days_Since_Prior_Order',
    'product_id': 'Product_ID',
    'product_name': 'Product_Name',
    'aisle_id': 'Aisle_ID',
    'aisle': 'Aisle',
    'department_id': 'Department_ID',
    'department': 'Department',
    'add_to_cart_order': 'Add_To_Cart_Order',
    'reordered': 'Reordered',
    'processed_data': 'Processed_Data'
} %}
{{ aliases[column] }}
{%- endmacro %}

{% macro medallion_quality_model(source_relation, columns, required_columns, primary_key, rejected=false) %}
with ranked_source as (
    select
        source_data.*,
        row_number() over (
            partition by
                {% for column in primary_key %}
                    source_data.{{ column }}{% if not loop.last %}, {% endif %}
                {% endfor %}
            order by
                source_data.processed_data desc nulls last,
                {% for column in columns %}
                    source_data.{{ column }}{% if not loop.last %}, {% endif %}
                {% endfor %}
        ) as _quality_row_number
    from {{ source_relation }} as source_data
),
classified_source as (
    select
        *,
        case
            when
                {% for column in required_columns %}
                    {{ column }} is null{% if not loop.last %} or {% endif %}
                {% endfor %}
                then 'NULL_REQUIRED_FIELD'
            when _quality_row_number > 1 then 'DUPLICATE_PRIMARY_KEY'
        end as _rejection_reason
    from ranked_source
)
select
    {% for column in columns %}
        {{ column }} as {{ pascal_case_column(column) }}{% if not loop.last %}, {% endif %}
    {% endfor %}
    {% if rejected %}
        , _rejection_reason as Rejection_Reason
        , current_timestamp() as Rejected_At
    {% endif %}
from classified_source
where {% if rejected %}_rejection_reason is not null{% else %}_rejection_reason is null{% endif %}
{% endmacro %}