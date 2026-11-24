select
    Order_ID,
    Product_ID,
    Dataset_Split
from {{ ref('fct_order_products') }}
group by Order_ID, Product_ID, Dataset_Split
having count(*) > 1