with
    employees as (
        select *
        from {{ ref('dim_employees') }}
    )

    , products as (
        select *
        from {{ ref('dim_products') }}
    )

    , int_sales as (
        select *
        from {{ ref('int_sales__order_items') }}
    )

    , joined_tables as (
        select
            int_sales.sk_order_item
            , int_sales.order_id
            , int_sales.order_customer_id
            , int_sales.order_employee_id
            , int_sales.order_detail_order_id
            , int_sales.order_detail_product_id
            , int_sales.order_date
            , int_sales.order_required_date
            , int_sales.order_shipped_date
            , int_sales.order_shipped_via
            , int_sales.order_freight_value
            , int_sales.order_ship_name
            , int_sales.order_ship_address
            , int_sales.order_ship_city
            , int_sales.order_ship_region
            , int_sales.order_ship_postal_code
            , int_sales.order_ship_country
            , int_sales.order_detail_unit_price
            , int_sales.order_detail_quantity
            , int_sales.order_detail_discount
            , products.product_name
            , products.product_quantity_per_unit
            , products.product_unit_price
            , products.product_units_in_stock
            , products.product_units_on_order
            , products.product_units_reorder_level
            , products.product_is_discontinued
            , products.category_name
            , products.supplier_name
            , products.supplier_country
            , employees.employee_name
            , employees.manager_name
            , employees.employee_title
            , employees.employee_birth_date
            , employees.employee_hire_date
        from int_sales
        left join products on
            int_sales.order_detail_product_id = products.product_id
        left join employees on
            int_sales.order_employee_id = employees.employee_id
    )

    , transformacoes as (
        select
            *
            , order_detail_quantity * order_detail_unit_price as gross_total
            , order_detail_quantity * order_detail_unit_price * (1 - order_detail_discount) as net_total
            , case
                when order_detail_discount > 0 then true
                else false
            end as order_item_has_discount
            , order_freight_value / count(order_id) over(partition by order_id) as weighted_freight_value
        from joined_tables
    )

    , select_final as (
        select
            /* Keys */
            sk_order_item
            , order_id
            , order_customer_id
            , order_employee_id
            , order_detail_product_id
            /* Dates */
            , order_date
            , order_required_date
            , order_shipped_date
            /* Metrics */
            , order_detail_unit_price
            , order_detail_quantity
            , order_detail_discount
            , weighted_freight_value
            , gross_total
            , net_total
            , order_item_has_discount
            /* Categories */
            , order_shipped_via
            , order_ship_name
            , order_ship_address
            , order_ship_city
            , order_ship_region
            , order_ship_country
            , product_name
            , product_quantity_per_unit
            , product_is_discontinued
            , category_name
            , supplier_name
            , supplier_country
            , employee_name
            , manager_name
            , employee_title
            , employee_birth_date
            , employee_hire_date
        from transformacoes
    )

select * from select_final