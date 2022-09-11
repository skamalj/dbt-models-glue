with grouped as (
    select count(*) as emp_count , id, dept 
    from flights.emp_raw 
    group by id, dept
)

select * from grouped where  emp_count > 1 order by 3