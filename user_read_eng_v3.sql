create table perlego_reporting_layer.ad_hoc.nb__user_read_eng_metric_ext2 as 
WITH base AS 
(
select 
date_trunc('month',registration_date) registration_month,
case when payment_model = 'direct' then 'd2c' else 'b2b' end user_type,
user_id, registration_date, user_persona, referrer_book_id,
b.country, b.continent, b.market, b.region
from perlego_reporting_layer.prod.dim__users a
left join perlego_reporting_layer.prod.dim__country_regions b on (a.country_id = b.country_id)
where registration_date >= date '2023-08-01'
and is_converted_subscribed = true
and lower(country) in ('united kingdom', 'united states')
),

subscription AS 
(
select user_id, min(subscription_start_time) as subscription_start_time, max(subscription_end_time) as subscription_end_time,
sum(tenure_months_todate) as tenure_months_todate
from (
select 
user_id, subscription_start_time, subscription_end_time,
CAST(CASE WHEN subscription_start_time IS NULL THEN 0 ELSE months_between(CASE WHEN DATE(subscription_end_time) = DATE '9999-12-31' THEN 
CAST(CURRENT_TIMESTAMP AS TIMESTAMP) ELSE  subscription_end_time END, subscription_start_time ) END AS INTEGER ) AS tenure_months_todate
from perlego_reporting_layer.prod.fct__subscriptions
where subscription_type = 'subscribed'
and user_id in (select user_id from base) ) group by 1
),

user_base_sub AS 
(
select 
a.*, b.subscription_start_time, b.subscription_end_time, b.tenure_months_todate,
coalesce(b.subscription_start_time,a.registration_date) as reg_sub_start_time,
case when coalesce(b.subscription_start_time,a.registration_date) = b.subscription_start_time then 'sub' else 'reg' end start_time_status
from base a left join subscription b on (a.user_id = b.user_id)
),

reading_eng AS 
(
select *  
from perlego_reporting_layer.prod.fct__reading_activity
where read_date >= date '2023-08-01'
and user_id in (select user_id from base) 
), 

event_eng as 
(
select *
from perlego_reporting_layer.prod.fct__events 
where event_date >= date '2023-08-01'
and event_name in ('highlight creation','annotation creation','open book','chapter load','page load','book opening')
and user_id in (select user_id from base) 
),




summary_read as
(
select 
b.*, 
case when active_wk_m1 >=2 then true else false end active_wk_flag_m1,
case when active_wk_m2 >=2 then true else false end active_wk_flag_m2,
case when active_wk_m3 >=2 then true else false end active_wk_flag_m3
from
(
select  
a.*, 

case when book_read_w1 >= 1 and reading_mins_w1 >= 5 then 1 else 0 end active_w1,
case when book_read_w1 >= 1 and reading_mins_w1 >= 5 then true else false end active_flag_w1,

case when book_read_w2 >= 1 and reading_mins_w2 >= 5 then 1 else 0 end active_w2,
case when book_read_w2 >= 1 and reading_mins_w2 >= 5 then true else false end active_flag_w2,

case when book_read_w3 >= 1 and reading_mins_w3 >= 5 then 1 else 0 end active_w3,
case when book_read_w3 >= 1 and reading_mins_w3 >= 5 then true else false end active_flag_w3,

case when book_read_w4 >= 1 and reading_mins_w4 >= 5 then 1 else 0 end active_w4,
case when book_read_w4 >= 1 and reading_mins_w4 >= 5 then true else false end active_flag_w4,

case when book_read_w5 >= 1 and reading_mins_w5 >= 5 then 1 else 0 end active_w5,
case when book_read_w5 >= 1 and reading_mins_w5 >= 5 then true else false end active_flag_w5,

case when book_read_w6 >= 1 and reading_mins_w6 >= 5 then 1 else 0 end active_w6,
case when book_read_w6 >= 1 and reading_mins_w6 >= 5 then true else false end active_flag_w6,

case when book_read_w7 >= 1 and reading_mins_w7 >= 5 then 1 else 0 end active_w7,
case when book_read_w7 >= 1 and reading_mins_w7 >= 5 then true else false end active_flag_w7,

case when book_read_w8 >= 1 and reading_mins_w8 >= 5 then 1 else 0 end active_w8,
case when book_read_w8 >= 1 and reading_mins_w8 >= 5 then true else false end active_flag_w8,

case when book_read_w9 >= 1 and reading_mins_w9 >= 5 then 1 else 0 end active_w9,
case when book_read_w9 >= 1 and reading_mins_w9 >= 5 then true else false end active_flag_w9,

case when book_read_w10 >= 1 and reading_mins_w10 >= 5 then 1 else 0 end active_w10,
case when book_read_w10 >= 1 and reading_mins_w10 >= 5 then true else false end active_flag_w10,

case when book_read_w11 >= 1 and reading_mins_w11 >= 5 then 1 else 0 end active_w11,
case when book_read_w11 >= 1 and reading_mins_w11 >= 5 then true else false end active_flag_w11,

case when book_read_w12 >= 1 and reading_mins_w12 >= 5 then 1 else 0 end active_w12,
case when book_read_w12 >= 1 and reading_mins_w12 >= 5 then true else false end active_flag_w12,


(
case when book_read_w1 >= 1 and reading_mins_w1 >= 5 then 1 else 0 end +
case when book_read_w2 >= 1 and reading_mins_w2 >= 5 then 1 else 0 end +
case when book_read_w3 >= 1 and reading_mins_w3 >= 5 then 1 else 0 end +
case when book_read_w4 >= 1 and reading_mins_w4 >= 5 then 1 else 0 end ) as active_wk_m1,

(
case when book_read_w5 >= 1 and reading_mins_w5 >= 5 then 1 else 0 end +
case when book_read_w6 >= 1 and reading_mins_w6 >= 5 then 1 else 0 end +
case when book_read_w7 >= 1 and reading_mins_w7 >= 5 then 1 else 0 end +
case when book_read_w8 >= 1 and reading_mins_w8 >= 5 then 1 else 0 end ) as active_wk_m2,

(
case when book_read_w9 >= 1 and reading_mins_w9 >= 5 then 1 else 0 end +
case when book_read_w10 >= 1 and reading_mins_w10 >= 5 then 1 else 0 end +
case when book_read_w11 >= 1 and reading_mins_w11 >= 5 then 1 else 0 end +
case when book_read_w12 >= 1 and reading_mins_w12 >= 5 then 1 else 0 end ) as active_wk_m3

from 
(
select 
a.registration_month, a.user_type,
a.user_id, a.registration_date, a.user_persona, a.referrer_book_id,a.country, a.continent, a.market, a.region,
a.subscription_start_time, a.subscription_end_time, a.tenure_months_todate,a.reg_sub_start_time,a.start_time_status,


/* Day (1-7)*/
count(distinct case when b.read_date = date(a.reg_sub_start_time) then b.book_id end)
AS book_read_d1,
sum(case when b.read_date = date(a.reg_sub_start_time) then b.reading_minutes end)
AS reading_mins_d1,
count(distinct case when b.read_date = date(date_add(a.reg_sub_start_time, 1)) then b.book_id end)
AS book_read_d2,
sum(case when b.read_date = date(date_add(a.reg_sub_start_time, 1)) then b.reading_minutes end)
AS reading_mins_d2,
count(distinct case when b.read_date = date(date_add(a.reg_sub_start_time, 2)) then b.book_id end)
AS book_read_d3,
sum(case when b.read_date = date(date_add(a.reg_sub_start_time, 2)) then b.reading_minutes end)
AS reading_mins_d3,
count(distinct case when b.read_date = date(date_add(a.reg_sub_start_time, 3)) then b.book_id end)
AS book_read_d4,
sum(case when b.read_date = date(date_add(a.reg_sub_start_time, 3)) then b.reading_minutes end)
AS reading_mins_d4,
count(distinct case when b.read_date = date(date_add(a.reg_sub_start_time, 4)) then b.book_id end)
AS book_read_d5,
sum(case when b.read_date = date(date_add(a.reg_sub_start_time, 4)) then b.reading_minutes end)
AS reading_mins_d5,
count(distinct case when b.read_date = date(date_add(a.reg_sub_start_time, 5)) then b.book_id end)
AS book_read_d6,
sum(case when b.read_date = date(date_add(a.reg_sub_start_time, 5)) then b.reading_minutes end)
AS reading_mins_d6,
count(distinct case when b.read_date = date(date_add(a.reg_sub_start_time, 6)) then b.book_id end)
AS book_read_d7,
sum(case when b.read_date = date(date_add(a.reg_sub_start_time, 6)) then b.reading_minutes end)
AS reading_mins_d7,


/* WEEK 1 */
count(distinct case when b.read_date between date(a.reg_sub_start_time) and date(date_add(a.reg_sub_start_time, 6)) then b.book_id end)
AS book_read_w1,
sum(case when b.read_date between date(a.reg_sub_start_time) and date(date_add(a.reg_sub_start_time, 6)) then b.reading_minutes end)
AS reading_mins_w1,
count(distinct case when b.read_date between date(a.reg_sub_start_time) and date(date_add(a.reg_sub_start_time, 6)) then b.read_date end)
AS reading_days_w1,

/* WEEK 2 */
count(distinct case when b.read_date between date(date_add(a.reg_sub_start_time, 7)) and date(date_add(a.reg_sub_start_time, 13)) then b.book_id end)
AS book_read_w2,
sum(case when b.read_date between date(date_add(a.reg_sub_start_time, 7)) and date(date_add(a.reg_sub_start_time, 13)) then b.reading_minutes end)
AS reading_mins_w2,
count(distinct case when b.read_date between date(date_add(a.reg_sub_start_time, 7)) and date(date_add(a.reg_sub_start_time, 13)) then b.read_date end)
AS reading_days_w2,



/* WEEK 3 */
count(distinct case when b.read_date between date(date_add(a.reg_sub_start_time, 14)) and date(date_add(a.reg_sub_start_time, 20)) then b.book_id end)
AS book_read_w3,
sum(case when b.read_date between date(date_add(a.reg_sub_start_time, 14)) and date(date_add(a.reg_sub_start_time, 20)) then b.reading_minutes end)
AS reading_mins_w3,
count(distinct case when b.read_date between date(date_add(a.reg_sub_start_time, 14)) and date(date_add(a.reg_sub_start_time, 20)) then b.read_date end)
AS reading_days_w3,

/* WEEK 4 */
count(distinct case when b.read_date between date(date_add(a.reg_sub_start_time, 21)) and date(date_add(a.reg_sub_start_time, 27)) then b.book_id end)
AS book_read_w4,
sum(case when b.read_date between date(date_add(a.reg_sub_start_time, 21)) and date(date_add(a.reg_sub_start_time, 27)) then b.reading_minutes end)
AS reading_mins_w4,
count(distinct case when b.read_date between date(date_add(a.reg_sub_start_time, 21)) and date(date_add(a.reg_sub_start_time, 27)) then b.read_date end)
AS reading_days_w4,

/* MONTH 1 */
sum(case when b.read_date between date(a.reg_sub_start_time) and date(date_add(a.reg_sub_start_time, 27)) then b.reading_minutes end)
AS reading_mins_m1,
count(distinct case when b.read_date between date(a.reg_sub_start_time) and date(date_add(a.reg_sub_start_time, 27)) then b.book_id end)
AS book_read_m1,
count(distinct case when b.read_date between date(a.reg_sub_start_time) and date(date_add(a.reg_sub_start_time, 27)) then b.read_date end)
AS reading_days_m1,
count(distinct case when b.read_date between date(a.reg_sub_start_time) and date(date_add(a.reg_sub_start_time, 27)) then date_trunc('week',b.read_date) end)
AS reading_weeks_m1,


/* WEEK 5 */
count(distinct case when b.read_date between date(date_add(a.reg_sub_start_time, 28)) and date(date_add(a.reg_sub_start_time, 34)) then b.book_id end)
AS book_read_w5,
sum(case when b.read_date between date(date_add(a.reg_sub_start_time, 28)) and date(date_add(a.reg_sub_start_time, 34)) then b.reading_minutes end)
AS reading_mins_w5,

/* WEEK 6 */
count(distinct case when b.read_date between date(date_add(a.reg_sub_start_time, 35)) and date(date_add(a.reg_sub_start_time, 41)) then b.book_id end)
AS book_read_w6,
sum(case when b.read_date between date(date_add(a.reg_sub_start_time, 35)) and date(date_add(a.reg_sub_start_time, 41)) then b.reading_minutes end)
AS reading_mins_w6,

/* WEEK 7 */
count(distinct case when b.read_date between date(date_add(a.reg_sub_start_time, 42)) and date(date_add(a.reg_sub_start_time, 48)) then b.book_id end)
AS book_read_w7,
sum(case when b.read_date between date(date_add(a.reg_sub_start_time, 42)) and date(date_add(a.reg_sub_start_time, 48)) then b.reading_minutes end)
AS reading_mins_w7,

/* WEEK 8 */
count(distinct case when b.read_date between date(date_add(a.reg_sub_start_time, 49)) and date(date_add(a.reg_sub_start_time, 55)) then b.book_id end)
AS book_read_w8,
sum(case when b.read_date between date(date_add(a.reg_sub_start_time, 49)) and date(date_add(a.reg_sub_start_time, 55)) then b.reading_minutes end)
AS reading_mins_w8,

/* MONTH 2*/
sum(case when b.read_date between date(date_add(a.reg_sub_start_time, 28)) and date(date_add(a.reg_sub_start_time, 55)) then b.reading_minutes end)
AS reading_mins_m2,
count(distinct case when b.read_date between date(date_add(a.reg_sub_start_time, 28)) and date(date_add(a.reg_sub_start_time, 55)) then b.book_id end)
AS book_read_m2,


/* WEEK 9 */
count(distinct case when b.read_date between date(date_add(a.reg_sub_start_time, 56)) and date(date_add(a.reg_sub_start_time, 62)) then b.book_id end)
AS book_read_w9,
sum(case when b.read_date between date(date_add(a.reg_sub_start_time, 56)) and date(date_add(a.reg_sub_start_time, 62)) then b.reading_minutes end)
AS reading_mins_w9,

/* WEEK 10 */
count(distinct case when b.read_date between date(date_add(a.reg_sub_start_time, 63)) and date(date_add(a.reg_sub_start_time, 69)) then b.book_id end)
AS book_read_w10,
sum(case when b.read_date between date(date_add(a.reg_sub_start_time, 63)) and date(date_add(a.reg_sub_start_time, 69)) then b.reading_minutes end)
AS reading_mins_w10,

/* WEEK 11 */
count(distinct case when b.read_date between date(date_add(a.reg_sub_start_time, 70)) and date(date_add(a.reg_sub_start_time, 76)) then b.book_id end)
AS book_read_w11,
sum(case when b.read_date between date(date_add(a.reg_sub_start_time, 70)) and date(date_add(a.reg_sub_start_time, 76)) then b.reading_minutes end)
AS reading_mins_w11,

/* WEEK 12 */
count(distinct case when b.read_date between date(date_add(a.reg_sub_start_time, 77)) and date(date_add(a.reg_sub_start_time, 83)) then b.book_id end)
AS book_read_w12,
sum(case when b.read_date between date(date_add(a.reg_sub_start_time, 77)) and date(date_add(a.reg_sub_start_time, 83)) then b.reading_minutes end)
AS reading_mins_w12,

/* MONTH 3 */
sum(case when b.read_date between date(date_add(a.reg_sub_start_time, 56)) and date(date_add(a.reg_sub_start_time, 83)) then b.reading_minutes end)
AS reading_mins_m3,
count(distinct case when b.read_date between date(date_add(a.reg_sub_start_time, 56)) and date(date_add(a.reg_sub_start_time, 83)) then b.book_id end)
AS book_read_m3,


/* This part of the script is the difference between nb__user_read_eng_metric_ext and nb__user_read_eng_metric_ext2 */
/* MONTH 1-3 */
count(distinct case when b.read_date between  date(a.reg_sub_start_time) and date(date_add(a.reg_sub_start_time, 83)) then b.book_id end)
AS book_read_m1_to_m3


from user_base_sub a
left join reading_eng b on (a.user_id = b.user_id)
group by 
a.registration_month, a.user_type,
a.user_id, a.registration_date, a.user_persona, a.referrer_book_id,a.country, a.continent, a.market, a.region,
a.subscription_start_time, a.subscription_end_time, a.tenure_months_todate,a.reg_sub_start_time,a.start_time_status
) a
) b
),



summary_event as
(
select 
a.user_id as user_id_ref, 

/* WEEK 1 */
count(distinct case when c.event_date between date(a.reg_sub_start_time) and date(date_add(a.reg_sub_start_time, 6)) and event_name = 'open book' then c.book_id end)
AS book_opened_w1,
count(distinct case when c.event_date between date(a.reg_sub_start_time) and date(date_add(a.reg_sub_start_time, 6)) and event_name in ('open book','chapter load','page load') then c.session_id end)
AS reading_session_w1,
count(distinct case when c.event_date between date(a.reg_sub_start_time) and date(date_add(a.reg_sub_start_time, 6)) and event_name = 'highlight creation' then c.event_id end)
AS highlight_w1,
count(distinct case when c.event_date between date(a.reg_sub_start_time) and date(date_add(a.reg_sub_start_time, 6)) and event_name = 'annotation creation' then c.event_id end)
AS annotation_w1,


/* WEEK 2 */
count(distinct case when c.event_date between date(date_add(a.reg_sub_start_time, 7)) and date(date_add(a.reg_sub_start_time, 13)) and event_name = 'open book' then c.book_id end)
AS book_opened_w2,
count(distinct case when c.event_date between date(date_add(a.reg_sub_start_time, 7)) and date(date_add(a.reg_sub_start_time, 13)) and event_name in ('open book','chapter load','page load') then c.session_id end)
AS reading_session_w2,
count(distinct case when c.event_date between date(date_add(a.reg_sub_start_time, 7)) and date(date_add(a.reg_sub_start_time, 13)) and event_name = 'highlight creation' then c.event_id end)
AS highlight_w2,
count(distinct case when c.event_date between date(date_add(a.reg_sub_start_time, 7)) and date(date_add(a.reg_sub_start_time, 13)) and event_name = 'annotation creation' then c.event_id end)
AS annotation_w2,


/* WEEK 3 */
count(distinct case when c.event_date between date(date_add(a.reg_sub_start_time, 14)) and date(date_add(a.reg_sub_start_time, 20)) and event_name = 'open book' then c.book_id end)
AS book_opened_w3,
count(distinct case when c.event_date between date(date_add(a.reg_sub_start_time, 14)) and date(date_add(a.reg_sub_start_time, 20)) and event_name in ('open book','chapter load','page load') then c.session_id end)
AS reading_session_w3,
count(distinct case when c.event_date between date(date_add(a.reg_sub_start_time, 14)) and date(date_add(a.reg_sub_start_time, 20)) and event_name = 'highlight creation' then c.event_id end)
AS highlight_w3,
count(distinct case when c.event_date between date(date_add(a.reg_sub_start_time, 14)) and date(date_add(a.reg_sub_start_time, 20)) and event_name = 'annotation creation' then c.event_id end)
AS annotation_w3,


/* WEEK 4 */
count(distinct case when c.event_date between date(date_add(a.reg_sub_start_time, 21)) and date(date_add(a.reg_sub_start_time, 27)) and event_name = 'open book' then c.book_id end)
AS book_opened_w4,
count(distinct case when c.event_date between date(date_add(a.reg_sub_start_time, 21)) and date(date_add(a.reg_sub_start_time, 27)) and event_name in ('open book','chapter load','page load') then c.session_id end)
AS reading_session_w4,
count(distinct case when c.event_date between date(date_add(a.reg_sub_start_time, 21)) and date(date_add(a.reg_sub_start_time, 27)) and event_name = 'highlight creation' then c.event_id end)
AS highlight_w4,
count(distinct case when c.event_date between date(date_add(a.reg_sub_start_time, 21)) and date(date_add(a.reg_sub_start_time, 27)) and event_name = 'annotation creation' then c.event_id end)
AS annotation_w4,


/* Month 1 */
count(distinct case when c.event_date between date(a.reg_sub_start_time) and date(date_add(a.reg_sub_start_time, 27)) and event_name in ('open book','chapter load','page load') then c.session_id end)
AS reading_session_m1,

/* Month 2 */
count(distinct case when c.event_date between date(date_add(a.reg_sub_start_time, 28)) and date(date_add(a.reg_sub_start_time, 55)) and event_name in ('open book','chapter load','page load') then c.session_id end)
AS reading_session_m2,

/* Month 3 */
count(distinct case when c.event_date between date(date_add(a.reg_sub_start_time, 56)) and date(date_add(a.reg_sub_start_time, 83)) and event_name in ('open book','chapter load','page load') then c.session_id end)
AS reading_session_m3

from user_base_sub a
left join event_eng c on (a.user_id = c.user_id)
group by 1
),


summary AS 
(
select 
a.*, 
b.*,
(reading_mins_m1 / reading_session_m1) reading_mins_per_session_m1,
(reading_mins_m2 / reading_session_m2) reading_mins_per_session_m2,
(reading_mins_m3 / reading_session_m3) reading_mins_per_session_m3
from summary_read a
left join summary_event b on (a.user_id = b.user_id_ref)
)


select *
from summary
;
