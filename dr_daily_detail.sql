{{ config(tags=['daily', 'dealer_reporting']) }}
{{ config(materialized='table', transient=true) }}

with item_stats as (        #daily sum across dealer items
    select
         dealer_id
        ,activity_date as date
        ,sum(organicviews) as total_views
        ,sum(engages) as engages
        ,sum(calls) as calls
        ,sum(boards) as boards
        ,sum(avg_call_sec * calls) as total_call_sec
        ,sum(calls_over_5min) as calls_over_5min
        ,max(longest_call) as longest_call
    from {{ref('dr_item_detail')}} as dr_item_detail
    group by 1,2
), med_eng as (
    select
    distinct 
        dealer_id
        ,local_date
        ,percentile_cont(seller_delay, 0.5) over (partition by dealer_id, local_date) as med_response_delay_8to8
    FROM {{ref('dr_engagement_detail')}} as dr_engagement_detail
    where local_hour >= 8 and local_hour <= 19
), response_stats as (      ##daily sum across dealer engagements
    select
         dr_engagement_detail.dealer_id
        ,dr_engagement_detail.local_date as date
        ,med_eng.med_response_delay_8to8
        ,avg(cbp) as cbp_pct
        ,avg(hip) as hip_pct
        ,avg(case when seller_delay is not null and seller_delay <= 172800 then 1 else 0 end) as response_rate --timeboxing this to 1 day so it is much more relevant during WBR for current week
        ,avg(case when sb.user_id is not null then null ##exclude from average calc when buyer was softblocked
                  when seller_delay is not null and seller_delay <= 172800 then 1 else 0 end) as response_rate_excl_sb
        ,avg(responses_in_24_hrs) as thread_length_first_24
        ,sum(1) as missed_engagements_denominator
        ,sum(case when (seller_delay > 172800 #2 days in seconds
                or seller_delay is null)
            then 1 else 0 end) as missed_engagements_numerator_total
        ,sum(case when (seller_delay > 172800 #2 daysin seconds
                or seller_delay is null)
                and hip = 1
            then 1 else 0 end) as missed_engagements_numerator_hip
        ,sum(case when (seller_delay > 172800 #2 daysin seconds
                or seller_delay is null)
                and cbp = 1
            then 1 else 0 end) as missed_engagements_numerator_cbp
    FROM {{ref('dr_engagement_detail')}} as dr_engagement_detail
    left outer join med_eng on med_eng.dealer_id = dr_engagement_detail.dealer_id and med_eng.local_date = dr_engagement_detail.local_date
    left outer join `prod-analytics-1312.marketing.user_softblock_list` as sb on sb.user_id = dr_engagement_detail.buyer_id
    group by 1,2,3
), var_mapping as (
    select
         ssa.id as salesforce_account_id
        ,ssa.ou_id_c as dealer_id
        ,ssa.value_added_reseller_c as var_id
        ,ssa2.name as var_name
    from `prod-fivetran-4ccc.salesforce.account` as ssa
    left outer join `prod-fivetran-4ccc.salesforce.account` as ssa2 on ssa2.id = ssa.value_added_reseller_c
        where ssa.value_added_reseller_c is not null
), minmax as (
    select
    distinct
         sa_sub_history.salesforce_account_id
        ,sa_sub_history.date
        ,sa_sub_history.ou_id as dealer_id
        ,min(sa_sub_history.date) over (partition by sa_sub_history.salesforce_account_id) as start_date
        ,max(sa_sub_history.date) over (partition by sa_sub_history.salesforce_account_id) as end_date
        ,sa_sub_history.mrr
        ,sa_sub_history.mrr_disc
    from {{ref('sa_sub_history')}} as sa_sub_history
    where sa_sub_history.mrr is not null    ##they have active sub on this date
    and sa_sub_history.ou_id is not null
), frame as (
    select
        *
        ,date_diff(end_date, start_date, day) as duration
    from minmax
), rolling_prelim as (
    select
         frame.*
        ,item_stats.total_views
        ,item_stats.engages
        ,item_stats.calls
        ,item_stats.boards
        ,item_stats.total_call_sec
        ,item_stats.calls_over_5min
        ,item_stats.longest_call
        ,sum(engages) over (partition by frame.dealer_id order by frame.date ROWS BETWEEN 6 PRECEDING and current row) as rolleng
        ,sum(calls) over (partition by frame.dealer_id order by frame.date ROWS BETWEEN 6 PRECEDING and current row) as rollcall
        ,avg(mrr) over (partition by frame.dealer_id order by frame.date ROWS BETWEEN 6 PRECEDING and current row) as rollmrr
        ,avg(mrr_disc) over (partition by frame.dealer_id order by frame.date ROWS BETWEEN 6 PRECEDING and current row) as rollmrr_disc
        ,lag(frame.date,6) over (partition by frame.dealer_id order by frame.date) as weekstart
        ,date_add(frame.date, interval -6 day) as weekstart_2
    from frame
    left join item_stats on item_stats.dealer_id = frame.dealer_id and item_stats.date = frame.date
), rolling_prelim_2 as (
    select
         *
        ,30.43667 as days_in_month  ##EXTRACT(DAY FROM DATE_SUB(DATE_TRUNC(DATE_ADD(date, INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY)) #updated to just avg days in month to smooth trends and remove questions about Feb variances
        ,case when weekstart = weekstart_2 then rolleng else null end as xrolleng   --not bridging 7 days looking back to pre-churn or gap in service
        ,case when weekstart = weekstart_2 then rollcall else null end as xrollcall
        ,case when weekstart = weekstart_2 then rollmrr else null end as xrollmrr
        ,case when weekstart = weekstart_2 then rollmrr_disc else null end as xrollmrr_disc
    from rolling_prelim
), rolling as (
    SELECT
        *
        ,case when weekstart <> weekstart_2 then null
            when (xrollmrr * (7 / days_in_month) / nullif((ifnull(xrolleng,0) + ifnull(xrollcall,0)),0)) is null and mrr is not null then 99 #if theyre paying us and got no engagements, thats an infinite cost per engagement
            else (xrollmrr * (7 / days_in_month) / nullif((ifnull(xrolleng,0) + ifnull(xrollcall,0)),0))
            end as rollcpe
        ,case when weekstart <> weekstart_2 then null
            when (xrollmrr_disc * (7 / days_in_month) / nullif((ifnull(xrolleng,0) + ifnull(xrollcall,0)),0)) is null and mrr is not null then 99 #if theyre paying us and got no engagements, thats an infinite cost per engagement
            else (xrollmrr_disc * (7 / days_in_month) / nullif((ifnull(xrolleng,0) + ifnull(xrollcall,0)),0))
            end as rollcpe_disc
    FROM rolling_prelim_2
), volume_history as (
    select
        date
        ,dealer_id
        ,dealer_feed_posted
        ,manual_posted
        ,asp
        ,alp
        ,feed_active
        ,manual_active
        ,feed_sold
        ,manual_sold
        ,sum(dealer_feed_posted) over (partition by dealer_id order by date ROWS BETWEEN 27 PRECEDING and current row) as rollpost28_feed
        ,sum(manual_posted) over (partition by dealer_id order by date ROWS BETWEEN 27 PRECEDING and current row) as rollpost28_manual
    from {{ref('dr_cars_volume_history')}}
), daily_mrr as (
    select
        ou_id
        ,date
        ,mrr
        ,case when mrr <> lag(mrr) over (partition by ou_id order by date asc) 
            and lag(mrr) over (partition by ou_id order by date asc) is not null
            then 1 else 0 end as mrr_change
    from {{ref('sa_sub_history')}}
    where ou_id is not null
), mrr_change as (
    select
        ou_id
        ,max(date) as most_recent_reprice
    from daily_mrr
    where mrr_change = 1
    group by 1
), promo_impressions as (
    select
         date_pst as date
        ,dealer_id
        ,sum(item_views) as total_views_promo24
        ,sum(impressions) as total_impressions_promo24
        ,count(1) as day_uptime_tiles
        ,sum(case when impressions = 0 then 1 else 0 end) as imp_0000
        ,sum(case when impressions <= 300 and impressions > 0 then 1 else 0 end) as imp_0300belo
        ,sum(case when impressions > 300 and impressions <= 600 then 1 else 0 end) as imp_03000600
        ,sum(case when impressions > 600 and impressions <= 900 then 1 else 0 end) as imp_06000900
        ,sum(case when impressions > 900 and impressions <= 1200 then 1 else 0 end) as imp_09001200
        ,sum(case when impressions > 1200 and impressions <= 1500 then 1 else 0 end) as imp_12001500
        ,sum(case when impressions > 1500 then 1 else 0 end) as imp_1500plus
    from {{ref('dr_impressions')}}
    where day_uptime = 86400
    group by 1,2
    order by 1 desc
), bump_performance as (
    select
        date(bump_time_pst) as date
        ,dealer_id
        ,sum(eng_first_day) as eng_first_day
        ,sum(views_first_day) as views_first_day
        ,sum(views_first_2) as views_first_2
    from {{ref('dr_bump_performance')}} as dr_bump_performance
    left outer join {{ref('dr_cars')}} as dr_cars on dr_cars.item_id = dr_bump_performance.item_id
    where dealer_id is not null
    group by 1,2
), sa_summary as (
    select
    distinct
        row_number() over (partition by ou_id order by sa.first_closed_won_date desc nulls last) as ou_number
        ,ou_id,first_closed_won_date,CHURN_DATE,active_product_purchased
        ,num_ads_purchased,pct_ads,sf_corrected_dealership_type,sf_dealer_cohorting,is_var
        ,is_delinquent,LEAD_SOURCE,chat_vendor,vendor_features,ZIPCODE,STATE_abbr,CITY,salesforce_lot_size
        ,name,account_manager_owner_name
    from {{ref('sa_summary')}} as sa
), near_final as (
    select
    distinct
         rolling.salesforce_account_id
        ,rolling.date
        ,rolling.days_in_month
        ,rolling.dealer_id
        ,rolling.start_date
        ,rolling.end_date
        ,rolling.duration
        ,case when  sa_summary.first_closed_won_date is not null then date_diff(rolling.date, sa_summary.first_closed_won_date, DAY) else null end as days_since_activation
        ,case when (sa_summary.churn_date is not null and date(sa_summary.churn_date) < current_date) then date_diff(date(sa_summary.churn_date), date(rolling.date), DAY) else null end as days_until_churn
        ,rolling.total_views
        ,rolling.engages
        ,rolling.calls
        ,rolling.boards
        ,rolling.total_call_sec
        ,rolling.calls_over_5min
        ,rolling.longest_call
        ,ifnull(volume_history.dealer_feed_posted,0) as dealer_feed_posted
        ,ifnull(volume_history.manual_posted,0) as manual_posted
        ,volume_history.asp
        ,volume_history.alp
        ,volume_history.feed_active
        ,volume_history.manual_active
        ,ifnull(volume_history.feed_sold,0) as feed_sold
        ,ifnull(volume_history.manual_sold,0) as manual_sold
        ,volume_history.rollpost28_feed
        ,volume_history.rollpost28_manual
        ,response_stats.hip_pct
        ,response_stats.cbp_pct
        ,response_stats.response_rate
        ,response_stats.response_rate_excl_sb
        ,response_stats.med_response_delay_8to8
        ,response_stats.thread_length_first_24
        ,response_stats.missed_engagements_denominator
        ,response_stats.missed_engagements_numerator_total
        ,response_stats.missed_engagements_numerator_hip
        ,response_stats.missed_engagements_numerator_cbp
        ,sa_summary.OU_ID
        ,sa_summary.first_closed_won_date
        ,sa_summary.CHURN_DATE
        ,sa_summary.active_product_purchased
        ,sa_summary.num_ads_purchased
        ,sa_summary.pct_ads
        ,sa_summary.sf_corrected_dealership_type
        ,sa_summary.sf_dealer_cohorting
        ,sa_summary.is_var
        ,var_mapping.var_name
        ,sa_summary.is_delinquent
        ,sa_summary.LEAD_SOURCE
        ,sa_summary.chat_vendor
        ,sa_summary.vendor_features
        ,sa_summary.ZIPCODE
        ,sa_summary.STATE_abbr
        ,sa_summary.CITY
        ,sa_summary.salesforce_lot_size
        ,replace(sa_summary.name, '/', '') as corrected_dealer_name
        ,ifnull(sa_summary.account_manager_owner_name, 'Blank AM Field') as AM
        ,dma.dma_name
        ,rolling.mrr as mrr
        ,rolling.mrr_disc as mrr_disc
        ,(rolling.mrr * (1 / days_in_month) / nullif((ifnull(item_stats.engages,0) + ifnull(item_stats.calls,0)),0)) as CPE --30.4 is avg days in month
        ,(rolling.mrr_disc * (1 / days_in_month) / nullif((ifnull(item_stats.engages,0) + ifnull(item_stats.calls,0)),0)) as CPE_disc --30.4 is avg days in month
        ,(rolling.mrr * (1 / days_in_month) / nullif(((ifnull(item_stats.engages,0) + ifnull(item_stats.calls,0)) * response_stats.response_rate),0)) as CPER --30.4 is avg days in month
        ,(rolling.mrr * (1 / days_in_month) / nullif((ifnull(volume_history.dealer_feed_posted,0) + ifnull(volume_history.manual_posted,0)),0)) as CPP --30.4 is avg days in month
        ,(rolling.mrr * (1000 / days_in_month) / nullif(item_stats.total_views,0)) as CPTV --cost per 1000 views
        ,rolling.xrolleng
        ,rolling.xrollcall
        ,rolling.xrollmrr
        ,rolling.xrollmrr_disc
        ,rolling.rollcpe
        ,rolling.rollcpe_disc
        ,count(1) over (partition by rolling.date) as dealer_count
        ,dr_promotions.uptime_manual
        ,dr_promotions.uptime_feed
        ,dr_promotions.expected
        ,dr_promotions.value_total
        ,(ifnull(dr_promotions.uptime_feed,0) + ifnull(dr_promotions.uptime_manual,0)) / nullif(least( (ifnull(volume_history.feed_active,0) + ifnull(volume_history.manual_active,0)), dr_promotions.expected),0) as value_total_w_floor
        ,dr_bumps.bumps_expected_currently
        ,dr_bumps.bumps_received
        ,mrr_change.most_recent_reprice
        ,promo_impressions.total_views_promo24
        ,promo_impressions.total_impressions_promo24
        ,promo_impressions.day_uptime_tiles
        ,promo_impressions.imp_0000
        ,promo_impressions.imp_0300belo
        ,promo_impressions.imp_03000600
        ,promo_impressions.imp_06000900
        ,promo_impressions.imp_09001200
        ,promo_impressions.imp_12001500
        ,promo_impressions.imp_1500plus
        ,bump_performance.eng_first_day
        ,bump_performance.views_first_day
        ,bump_performance.views_first_2
        ,ifnull(dr_profile_detail.profile_views,0) as profile_views
        ,ifnull(dr_profile_detail.url_clicks,0) as url_clicks
    from rolling
    left join item_stats on cast(item_stats.dealer_id as string) = cast(rolling.dealer_id as string) and item_stats.date = rolling.date
    left join response_stats on cast(response_stats.dealer_id as string) = cast(rolling.dealer_id as string) and response_stats.date = rolling.date
    left join var_mapping on cast(var_mapping.dealer_id as string) = cast(rolling.dealer_id as string)
    left join sa_summary on cast(sa_summary.ou_id as string) = cast(rolling.dealer_id as string) and sa_summary.ou_number = 1
    left join {{ref('dr_promotions')}} as dr_promotions on cast(dr_promotions.dealer_id as string) = cast(rolling.dealer_id as string) and dr_promotions.date = rolling.date
    left join {{ref('dr_bumps')}} as dr_bumps on cast(dr_bumps.dealer_id as string) = cast(rolling.dealer_id as string) and dr_bumps.date = rolling.date
    left outer join {{ref('dr_profile_detail')}} as dr_profile_detail on safe_cast(dr_profile_detail.dealer_id as int64) = safe_cast(rolling.dealer_id as int64) and dr_profile_detail.view_date = rolling.date
    left join volume_history on cast(volume_history.dealer_id as string) = cast(rolling.dealer_id as string) and volume_history.date = rolling.date
    left join `prod-analytics-1312.marketing.zip_to_dma` as dma on dma.zip = sa_summary.zipcode
    left outer join mrr_change on mrr_change.ou_id = rolling.dealer_id
    left outer join promo_impressions on cast(promo_impressions.dealer_id as int64) = rolling.dealer_id and promo_impressions.date = rolling.date
    left outer join bump_performance on bump_performance.dealer_id = rolling.dealer_id and bump_performance.date = rolling.date
)
    SELECT
        *
        ,row_number() over (partition by date order by CPE desc NULLS FIRST) as CPE_rank
        ,case when CPE is null
              then null
              else (row_number() over (partition by date order by CPE desc NULLS FIRST)) / dealer_count end as CPE_perc
    FROM near_final