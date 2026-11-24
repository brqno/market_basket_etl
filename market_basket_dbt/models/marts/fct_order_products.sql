with Order_Products as (
    select *, 'prior' as Dataset_Split from {{ ref('stg_order_products_prior') }}
    union all
    select *, 'train' as Dataset_Split from {{ ref('stg_order_products_train') }}
)

select
    Order_Products.Order_ID as Order_ID,
    Orders.User_ID as User_ID,
    Order_Products.Product_ID as Product_ID,
    Products.Product_Name as Product_Name,
    Products.Aisle_ID as Aisle_ID,
    Products.Aisle as Aisle,
    Products.Department_ID as Department_ID,
    Products.Department as Department,
    Order_Products.Add_To_Cart_Order as Add_To_Cart_Order,
    Order_Products.Reordered as Reordered,
    Order_Products.Dataset_Split as Dataset_Split
from Order_Products
left join {{ ref('stg_orders') }} as Orders
    on Order_Products.Order_ID = Orders.Order_ID
left join {{ ref('dim_products') }} as Products
    on Order_Products.Product_ID = Products.Product_ID