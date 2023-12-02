with
    source_orders as (
        select
            cast(order_id as int) as order_id
            , cast(customer_id as string) as order_customer_id
            , cast(employee_id as int) as order_employee_id
            , cast(order_date as string) as order_date
            , cast(required_date as date) as order_required_date
            , cast(shipped_date as date) as order_shipped_date
            , cast(ship_via as string) as order_shipped_via
            , cast(freight as numeric) as order_freight_value
            , cast(ship_name as string) as order_ship_name
            , cast(ship_address as string) as order_ship_address
            , cast(ship_city as string) as order_ship_city
            , cast(ship_region as string) as order_ship_region
            , cast(ship_postal_code as string) as order_ship_postal_code
            , cast(ship_country as string) as order_ship_country
        from {{ source('erp', 'orders') }}
    )

select *
from source_orders