with
    source_products as (
        select
            cast(product_id as int) as product_id
            , cast(supplier_id as int) as product_supplier_id
            , cast(category_id as int) as product_category_id
            , cast(product_name as string) as product_name
            , cast(quantity_per_unit as string) as product_quantity_per_unit
            , cast(unit_price as numeric) as product_unit_price
            , cast(units_in_stock as int) as product_units_in_stock
            , cast(units_on_order as int) as product_units_on_order
            , cast(reorder_level as int) as product_units_reorder_level
            , case
                when discontinued = 1 then true
                else false
            end as product_is_discontinued
        from {{ source('erp', 'products') }}
    )

select *
from source_products