with
    source_suppliers as (
        select
            cast(supplier_id as int) as supplier_id
            , cast(company_name as string) as supplier_name
            , cast(contact_name as string) as supplier_contact_name
            , cast(contact_title as string) as supplier_contact_title
            , cast(address as string) as supplier_address
            , cast(city as string) as supplier_city
            , cast(region as string) as supplier_region
            , cast(postal_code as string) as supplier_postal_code
            , cast(country as string) as supplier_country
            , cast(phone as string) as supplier_phone
        from {{ source('erp', 'suppliers') }}
    )

select *
from source_suppliers