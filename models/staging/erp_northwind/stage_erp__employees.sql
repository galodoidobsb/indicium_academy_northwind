with
    source_employees as (
        select
            cast(employee_id as int) as employee_id
            , cast(reports_to as int) as employee_manager_id
            , cast(first_name as string) || " " || cast(last_name as string) as employee_name
            , cast(title as string) as employee_title
            , cast(birth_date as date) as employee_birth_date
            , cast(hire_date as date) as employee_hire_date
            , cast(city as string) as employee_city
            , cast(region as string) as employee_region
            , cast(country as string) as employee_country
            , cast(notes as string) as employee_notes
        from {{ source('erp', 'employees') }}
    )

select *
from source_employees
order by employee_id