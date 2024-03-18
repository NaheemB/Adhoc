------- nb__learning_kpi_data_test

WITH base AS 
(
select 
date_trunc('month',registration_date) registration_month,
registration_date,
user_id
from perlego_reporting_layer.prod.dim__users a
where registration_date >= date '2023-08-01'
and is_converted_paid = true
),

subscription AS 
(
select user_id, min(subscription_start_time) as subscription_start_time, max(subscription_end_time) as subscription_end_time,
sum(tenure_months) as tenure_months
from (
select 
user_id, subscription_start_time, subscription_end_time,
CAST(CASE WHEN subscription_start_time IS NULL THEN 0 ELSE months_between(CASE WHEN DATE(subscription_end_time) = DATE '9999-12-31' THEN 
CAST(CURRENT_TIMESTAMP AS TIMESTAMP) ELSE  subscription_end_time END, subscription_start_time ) END AS INTEGER ) AS tenure_months
from perlego_reporting_layer.prod.fct__subscriptions
where subscription_type = 'subscribed'
and user_id in (select user_id from base) ) group by 1
),


user_base_sub AS 
(
select
a.*,
CAST(months_between( CAST(CURRENT_TIMESTAMP AS TIMESTAMP), reg_sub_start_time)AS INTEGER )  as tenure_months_expected
from
(
select 
a.*, b.subscription_start_time, b.subscription_end_time, b.tenure_months,
coalesce(b.subscription_start_time,a.registration_date) as reg_sub_start_time,
case when coalesce(b.subscription_start_time,a.registration_date) = b.subscription_start_time then 'sub' else 'reg' end start_time_status
from base a left join subscription b on (a.user_id = b.user_id)
) a
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


summary_reading as
(
select 
a.registration_month,
a.user_id, a.registration_date, a.subscription_start_time, a.subscription_end_time, coalesce(a.tenure_months,0) tenure_months,
a.reg_sub_start_time,a.start_time_status, a.tenure_months_expected,

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



/* WEEK 13 */
count(distinct case when b.read_date between date(date_add(a.reg_sub_start_time, 84)) and date(date_add(a.reg_sub_start_time, 90)) then b.book_id end)
AS book_read_w13,
sum(case when b.read_date between date(date_add(a.reg_sub_start_time, 84)) and date(date_add(a.reg_sub_start_time, 90)) then b.reading_minutes end)
AS reading_mins_w13,

/* WEEK 14 */
count(distinct case when b.read_date between date(date_add(a.reg_sub_start_time, 91)) and date(date_add(a.reg_sub_start_time, 97)) then b.book_id end)
AS book_read_w14,
sum(case when b.read_date between date(date_add(a.reg_sub_start_time, 91)) and date(date_add(a.reg_sub_start_time, 97)) then b.reading_minutes end)
AS reading_mins_w14,

/* WEEK 15 */
count(distinct case when b.read_date between date(date_add(a.reg_sub_start_time, 98)) and date(date_add(a.reg_sub_start_time, 104)) then b.book_id end)
AS book_read_w15,
sum(case when b.read_date between date(date_add(a.reg_sub_start_time, 98)) and date(date_add(a.reg_sub_start_time, 104)) then b.reading_minutes end)
AS reading_mins_w15,

/* WEEK 16 */
count(distinct case when b.read_date between date(date_add(a.reg_sub_start_time, 105)) and date(date_add(a.reg_sub_start_time, 111)) then b.book_id end)
AS book_read_w16,
sum(case when b.read_date between date(date_add(a.reg_sub_start_time, 105)) and date(date_add(a.reg_sub_start_time, 111)) then b.reading_minutes end)
AS reading_mins_w16,

/* MONTH 4 */
sum(case when b.read_date between date(date_add(a.reg_sub_start_time, 84)) and date(date_add(a.reg_sub_start_time, 111)) then b.reading_minutes end)
AS reading_mins_m4,
count(distinct case when b.read_date between date(date_add(a.reg_sub_start_time, 84)) and date(date_add(a.reg_sub_start_time, 111)) then b.book_id end)
AS book_read_m4,



/* MONTH 1-4 */
count(distinct case when b.read_date between  date(a.reg_sub_start_time) and date(date_add(a.reg_sub_start_time, 111)) then b.book_id end)
AS book_read_m1_to_m4


from user_base_sub a
left join reading_eng b on (a.user_id = b.user_id)
group by 
a.registration_month,
a.user_id, a.registration_date,
a.subscription_start_time, a.subscription_end_time,coalesce(a.tenure_months,0),a.reg_sub_start_time,a.start_time_status,a.tenure_months_expected
),



summary_read as 
(
select b.*,
(power_active_m1+power_active_m2+power_active_m3+power_active_m4) as power_active_m1_to_m4
from
(
select a.*,

case when book_read_m1_to_m4 >=3 then true else false end flag_3plus_m1_m4,

case when 
(case when reading_mins_w1 >= 72 then 1 else 0 end +
case when reading_mins_w2 >=  72 then 1 else 0 end +
case when reading_mins_w3 >=  72 then 1 else 0 end +
case when reading_mins_w3 >=  72 then 1 else 0 end
) >=2 then 1 else 0 end power_active_m1,

case when 
(case when reading_mins_w5 >= 72 then 1 else 0 end +
case when reading_mins_w6 >=  72 then 1 else 0 end +
case when reading_mins_w7 >=  72 then 1 else 0 end +
case when reading_mins_w8 >=  72 then 1 else 0 end
) >=2 then 1 else 0 end power_active_m2,

case when 
(case when reading_mins_w9 >= 72 then 1 else 0 end +
case when reading_mins_w10 >=  72 then 1 else 0 end +
case when reading_mins_w11 >=  72 then 1 else 0 end +
case when reading_mins_w12 >=  72 then 1 else 0 end
) >=2 then 1 else 0 end power_active_m3,


case when 
(case when reading_mins_w13 >= 72 then 1 else 0 end +
case when reading_mins_w14 >=  72 then 1 else 0 end +
case when reading_mins_w15 >=  72 then 1 else 0 end +
case when reading_mins_w16 >=  72 then 1 else 0 end
) >=2 then 1 else 0 end power_active_m4

from summary_reading a
) b
),



summary_event as
(
select 
a.user_id as user_id_ref, 

/* DAY 1 */
count(distinct case when c.event_date = date(a.reg_sub_start_time) and event_name = 'open book' then c.book_id end)
AS book_opened_d1,
count(distinct case when c.event_date = date(a.reg_sub_start_time) and event_name in ('open book','chapter load','page load') then c.session_id end)
AS reading_session_d1,
count(distinct case when c.event_date = date(a.reg_sub_start_time) and event_name = 'highlight creation' then c.event_id end)
AS highlight_d1,
count(distinct case when c.event_date = date(a.reg_sub_start_time) and event_name = 'annotation creation' then c.event_id end)
AS annotation_d1,

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
count(distinct case when c.event_date between date(a.reg_sub_start_time) and date(date_add(a.reg_sub_start_time, 27)) and event_name = 'open book' then c.book_id end)
AS book_opened_m1,
count(distinct case when c.event_date between date(a.reg_sub_start_time) and date(date_add(a.reg_sub_start_time, 27)) and event_name in ('open book','chapter load','page load') then c.session_id end)
AS reading_session_m1,
count(distinct case when c.event_date between  date(a.reg_sub_start_time) and date(date_add(a.reg_sub_start_time, 27)) and event_name = 'highlight creation' then c.event_id end)
AS highlight_m1,
count(distinct case when c.event_date between  date(a.reg_sub_start_time) and date(date_add(a.reg_sub_start_time, 27)) and event_name = 'annotation creation' then c.event_id end)
AS annotation_m1


from user_base_sub a
left join event_eng c on (a.user_id = c.user_id)
group by 1
),

summary as 
(
select 
a.*,
b.*,
case when highlight_d1 >0 then true else false end highlight_d1_flag,
case when annotation_d1 >0 then true else false end annotation_d1_flag,
case when highlight_d1>0 and annotation_d1 >0 then true else false end ugc_d1_flag,

case when highlight_w1 >0 then true else false end highlight_w1_flag,
case when annotation_w1 >0 then true else false end annotation_w1_flag,
case when highlight_w1>0 and annotation_w1 >0 then true else false end ugc_w1_flag,

case when highlight_w2 >0 then true else false end highlight_w2_flag,
case when annotation_w2 >0 then true else false end annotation_w2_flag,
case when highlight_w2 >0 and annotation_w2 >0 then true else false end ugc_w2_flag,

case when highlight_w3 >0 then true else false end highlight_w3_flag,
case when annotation_w3 >0 then true else false end annotation_w3_flag,
case when highlight_w3 >0 and annotation_w3 >0 then true else false end ugc_w3_flag,

case when highlight_w4 >0 then true else false end highlight_w4_flag,
case when annotation_w4 >0 then true else false end annotation_w4_flag,
case when highlight_w4 >0 and annotation_w3 >0 then true else false end ugc_w4_flag,

case when highlight_m1 >0 then true else false end highlight_m1_flag,
case when annotation_m1 >0 then true else false end annotation_m1_flag,
case when highlight_m1>0 and annotation_w1 >0 then true else false end ugc_m1_flag

from summary_read a
left join summary_event b on (a.user_id = b.user_id_ref)
)

select * 
from summary
