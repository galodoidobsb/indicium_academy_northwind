with
    source_order_details as (
        select
            cast(order_id as int) as order_detail_order_id
            , cast(product_id as int) as order_detail_product_id
            , cast(unit_price as numeric) as order_detail_unit_price
            , cast(quantity as int) as order_detail_quantity
            , cast(discount as numeric) as order_detail_discount
        from {{ source('erp', 'order_details') }}
    )

select *
from source_order_details