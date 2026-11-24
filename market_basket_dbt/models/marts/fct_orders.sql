with Basket_Sizes as (
    select Order_ID, count(*) as Product_Count
    from {{ ref('fct_order_products') }}
    group by Order_ID
)

select
    Orders.Order_ID as Order_ID,
    Orders.User_ID as User_ID,
    Orders.Eval_Set as Eval_Set,
    Orders.Order_Number as Order_Number,
    Orders.Order_DOW as Order_DOW,
    Orders.Order_Hour_Of_Day as Order_Hour_Of_Day,
    Orders.Days_Since_Prior_Order as Days_Since_Prior_Order,
    coalesce(Basket_Sizes.Product_Count, 0) as Product_Count
from {{ ref('stg_orders') }} as Orders
left join Basket_Sizes
    on Orders.Order_ID = Basket_Sizes.Order_ID