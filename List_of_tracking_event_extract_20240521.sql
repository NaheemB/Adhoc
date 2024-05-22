select 
a.*, 
date_diff(DAY,TIMESTAMP(last_event_timestamp),TIMESTAMP(current_date)) Last_time_event_was_received 
from (
select 
event_name, environment_name , count(1) cnt, min(record_date) first_event_timestamp, max(record_date) last_event_timestamp
from perlego_data_platform.prod.gold_event_tracking
where record_date <= '2024-05-21'
group by 1,2 order by 3 desc
limit 475
) a
order by 1
;
