select
    Products.Product_ID as Product_ID,
    Products.Product_Name as Product_Name,
    Products.Aisle_ID as Aisle_ID,
    Aisles.Aisle as Aisle,
    Products.Department_ID as Department_ID,
    Departments.Department as Department
from {{ ref('stg_products') }} as Products
left join {{ ref('stg_aisles') }} as Aisles
    on Products.Aisle_ID = Aisles.Aisle_ID
left join {{ ref('stg_departments') }} as Departments
    on Products.Department_ID = Departments.Department_ID