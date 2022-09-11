select count(*) from flights.hudi_emp_cow where update_hudi_ts = (
select max(update_hudi_ts) from flights.hudi_emp_cow)