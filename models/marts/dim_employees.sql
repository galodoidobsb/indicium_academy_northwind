with
    staging_employees as (
        select
            employee_id
            , employee_manager_id
            , employee_name
            , employee_title
            , employee_birth_date
            , employee_hire_date
            , employee_city
            , employee_region
            , employee_country
            , employee_notes
        from {{ ref('stage_erp__employees') }}
    )

    , self_join_employees as (
        select
            employees.employee_id
            , employees.employee_manager_id
            , employees.employee_name
            , managers.employee_name as manager_name
            , employees.employee_title
            , employees.employee_birth_date
            , employees.employee_hire_date
            , employees.employee_city
            , employees.employee_region
            , employees.employee_country
            , employees.employee_notes
        from staging_employees as employees
        left join staging_employees as managers on
            employees.employee_manager_id = managers.employee_id
    )

select *
from self_join_employees
order by employee_id