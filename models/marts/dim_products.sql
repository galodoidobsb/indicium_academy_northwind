with
    staging_categories as (
        select
            category_id
            , category_name
            , category_description
        from {{ ref('stage_erp__categories') }}
    )

    , staging_suppliers as (
        select
            supplier_id
            , supplier_name
            , supplier_contact_name
            , supplier_contact_title
            , supplier_address
            , supplier_city
            , supplier_region
            , supplier_country
        from {{ ref('stage_erp__suppliers') }}

    )

    , staging_products as (
        select
            product_id
            , product_supplier_id
            , product_category_id
            , product_name
            , product_quantity_per_unit
            , product_unit_price
            , product_units_in_stock
            , product_units_on_order
            , product_units_reorder_level
            , product_is_discontinued
        from {{ ref('stage_erp__products') }}
    )

    , joined_tables as (
        select
            product_id
            , product_supplier_id
            , product_category_id
            , product_name
            , product_quantity_per_unit
            , product_unit_price
            , product_units_in_stock
            , product_units_on_order
            , product_units_reorder_level
            , product_is_discontinued
            , category_name
            , supplier_name
            , supplier_country
        from staging_products
        left join staging_categories on staging_products.product_category_id = staging_categories.category_id
        left join staging_suppliers on staging_products.product_supplier_id = staging_suppliers.supplier_id
    )

select *
from joined_tables
order by product_id