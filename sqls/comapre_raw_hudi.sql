(
    select id,dept,eventtime, 'raw' as source 
    from flights.emp_raw where id = 83
)
UNION
(
    select id,dept,eventtime, 'hudi' as source 
    from flights.hudi_emp_cow where id = 83
)
order by 4 desc, 3 desc