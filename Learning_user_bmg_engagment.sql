------- STEP 1 = enrich the user_reg table 
create table athena_analytics.nb_act_ret_user_reg_refine as
with user_reg as
(
select
date_trunc('month', registration_time) as registration_month,
registration_time, user_id, payment_channel_crude,internal_user,converted_trial,converted_subscribed,converted_paid,
acquisition_country,acquisition_region,user_persona,academic_institution,organisation_id,organisation_name,
utm_source, utm_medium,referrer_book,internal_persona,platform_status,country_language, user_language
from analytics_reporting.user_registrations
),
discipline as
(
select user_id , academic_discipline
from reporting_layer.view_user_ref_academic_discipline
),
subscription as
(
select
user_id,
min(coalesce(start_timestamp,start_timestamp)) sub_start_timestamp,
max(coalesce(end_timestamp,paid_end_timestamp)) sub_end_timestamp,
sum(coalesce(subscribed_months,0)) subscribed_months
from analytics_reporting.subscriptions
where subscription_type = 'subscribed'
group by 1
)
select
case
when a.registration_month = cast('2022-09-01' as date) then 'Sept'
when a.registration_month = cast('2022-10-01' as date) then 'Oct'
when a.registration_month = cast('2023-09-01' as date) then 'Sept'
when a.registration_month = cast('2023-10-01' as date) then 'Oct' end cohort_month,
case
when a.registration_month = cast('2022-09-01' as date) then '2022'
when a.registration_month = cast('2022-10-01' as date) then '2022'
when a.registration_month = cast('2023-09-01' as date) then '2023'
when a.registration_month = cast('2023-10-01' as date) then '2023' end cohort_year,
case
when a.registration_month = cast('2022-09-01' as date) then 'Sept-2022'
when a.registration_month = cast('2022-10-01' as date) then 'Oct-2022'
when a.registration_month = cast('2023-09-01' as date) then 'Sept-2023'
when a.registration_month = cast('2023-10-01' as date) then 'Oct-2023' end cohort_period,
a.*, coalesce(b.academic_discipline,'Unknown') as academic_discipline,
c.sub_start_timestamp, c.sub_end_timestamp, c.subscribed_months
from user_reg a
left join discipline b on (a.user_id = b.user_id)
left join subscription c on (a.user_id = c.user_id)
where (registration_month = cast('2022-09-01' as date) or registration_month = cast('2022-10-01' as date)
or registration_month = cast('2023-09-01' as date) or registration_month = cast('2023-10-01' as date))
and converted_subscribed = 'Y'
;
-----insert data int reference table base (to easily create filter)
-- Creates the actual folder on S3
CREATE EXTERNAL TABLE IF NOT EXISTS reference_tables.ref_act_ret_user_reg_base (
cohort_month string,
cohort_year string,
cohort_period string,
registration_month timestamp,
registration_time timestamp,
user_id int,
payment_channel_crude string,
internal_user string,
converted_trial string,
converted_subscribed string,
converted_paid string,
acquisition_country string,
acquisition_region string,
user_persona string,
academic_institution string,
organisation_id int,
organisation_name string,
utm_source string,
utm_medium string,
referrer_book int,
internal_persona string,
platform_status string,
country_language string,
user_language string,
academic_discipline string,
sub_start_timestamp timestamp,
sub_end_timestamp timestamp,
subscribed_months bigint
)
LOCATION 's3://perlego-s3-reference-tb/reference_tables/ref_act_ret_user_reg_base/'
TBLPROPERTIES ('has_encrypted_data'='false');
insert into reference_tables.ref_act_ret_user_reg_base
select * from athena_analytics.nb_act_ret_user_reg_refine;
and {{cohort_month}}
and {{region}}
and {{user_persona}}
and {{user_type}}
and {{academic_discipline}}
and {{platform}}
--------- STEP 2 == create base for bmg list
create table athena_analytics.nb_bmg_reading_list as
select
perlego_bookid
from analytics_reporting.bmg_matched_data
where (available_status = 'Yes' OR bmg_isbn in (select bmg_isbn from athena_analytics.nb_ref_bmg_wiley_cleaup))
;
--------- STEP 3 == create data extract for reading activity
create table athena_analytics.nb_read_end_sept_nov_2022 as
select
*
from analytics_reporting.reading_activity_daily
where cast(date_format(reading_date ,'%Y%m%d') as int) between 20220901 and 20221130
;
create table athena_analytics.nb_read_end_sept_nov_2023 as
select
*
from analytics_reporting.reading_activity_daily
where cast(date_format(reading_date ,'%Y%m%d') as int) between 20230901 and 20231130
;
---------- STEP 4 == create data extract for reading session
create table athena_analytics.nb_read_sess_sept_nov_2022 as
select
*
from analytics_reporting.user_tracking_events
where cast(date_format(event_timestamp ,'%Y%m%d') as int) between 20220901 and 20221130
and event_name_master in (
'open book','annotation creation','blur reader','focus reader','book reference copied','bookmark creation','chapter click from within book',
'copy text','eye saver mode change','font family change','font size change','highlight creation','highlight removal',
'open chapter tab','page or chapter load')
;
create table athena_analytics.nb_read_sess_sept_nov_2023 as
select
*
from analytics_reporting.user_tracking_events
where cast(date_format(event_timestamp ,'%Y%m%d') as int) between 20230901 and 20231130
and event_name_master in (
'open book','annotation creation','blur reader','focus reader','book reference copied','bookmark creation','chapter click from within book',
'copy text','eye saver mode change','font family change','font size change','highlight creation','highlight removal',
'open chapter tab','page or chapter load')
;
# Info
- The goal is to create a subscriber retention strategy based around the different types of learning (research, assigned reading & general knowledge). The learning tribe needs to understand the natural frequency for each of the learning types, which would allow them build an intuition of what is 'expected' and 'good'.
- The dashboard comprises of 2 learning types, **Assigned Reading 👩‍🏫 **   &  **Research 📕** which captures:
  - **Active Reading Days (ARD)**:  
1. Distribution of users Active Reading Days (ARD) within the usage period & the ARD median.
2. The trend on daily active readers by segment (Below & Above ARD) median .
3. The active readers by their subscription week period for both Sept and Oct cohort.
   - **Active Reading Session (ARS)**:
4. Distribution of users overall Active Reading Session (ARS) broken-down into the number of books read (book category: 1,2,3+)  within the usage period & their corresponding  ARS median.
5. The Total Reading Session & Session Per User  by users subscription week period broken-down the number of books read (book category: 1,2,3+).
