with
    staging_orders as (
        select *
        from {{ ref('stage_erp__orders') }}
    )

    , staging_order_details as (
        select *
        from {{ ref('stage_erp__order_details') }}
    )

    , joined_tables as (
        select
        staging_orders.order_id
        , staging_orders.order_customer_id
        , staging_orders.order_employee_id
        , staging_orders.order_date
        , staging_orders.order_required_date
        , staging_orders.order_shipped_date
        , staging_orders.order_shipped_via
        , staging_orders.order_freight_value
        , staging_orders.order_ship_name
        , staging_orders.order_ship_address
        , staging_orders.order_ship_city
        , staging_orders.order_ship_region
        , staging_orders.order_ship_postal_code
        , staging_orders.order_ship_country
        , staging_order_details.order_detail_order_id
        , staging_order_details.order_detail_product_id
        , staging_order_details.order_detail_unit_price
        , staging_order_details.order_detail_quantity
        , staging_order_details.order_detail_discount
        from staging_order_details
        left join staging_orders on
        order_detail_order_id = order_id
    )

    ,criar_chave as (
        select
            cast(order_id as string) || '-' || cast(order_detail_product_id as string) as sk_order_item
            , *
        from joined_tables
    )

select *
from criar_chave