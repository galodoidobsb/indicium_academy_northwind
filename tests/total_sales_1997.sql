--DECLARE expected_1997_sales NUMERIC
--DECLARE tolerance NUMERIC

--set expected_1997_sales = 658388.75
--set tolerance = 1

with
    sales_1997 as (
        select sum(gross_total) as gross_total_sales
        from {{ ref('fact_sales') }}
        where order_date between '1997-01-01' and '1997-12-31'
    )

select gross_total_sales
from sales_1997
where gross_total_sales not between 658387 and 658389
--where gross_total_sales not between (expected_1997_sales - tolerance) and (expected_1997_sales + tolerance)