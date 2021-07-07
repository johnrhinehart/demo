with daily_mrr as (
    select
        ou_id
        ,date
        ,mrr
        ,lag(mrr) over (partition by ou_id order by date asc) as previous_mrr
        ,case when mrr <> lag(mrr) over (partition by ou_id order by date asc) 
            and lag(mrr) over (partition by ou_id order by date asc) is not null
            then 1 else 0 end as mrr_change
        ,case when mrr > lag(mrr) over (partition by ou_id order by date asc) 
            and lag(mrr) over (partition by ou_id order by date asc) is not null
            then 1 else 0 end as mrr_upsell
        ,case when mrr < lag(mrr) over (partition by ou_id order by date asc) 
            and lag(mrr) over (partition by ou_id order by date asc) is not null
            then 1 else 0 end as mrr_downsell
    from `prod-analytics-1312.dbt_analytics.sa_sub_history`
    where ou_id is not null
), mrr_change as (
    select
        ou_id
        ,date
        ,mrr
        ,previous_mrr
        ,sum(mrr_change) as mrr_change
        ,sum(mrr_upsell) as mrr_upsell
        ,sum(mrr_downsell) as mrr_downsell
    from daily_mrr
    where mrr_change = 1
        and date >= '2020-01-01'
    group by 1,2,3,4 
), days as (
    select
    distinct
        cal_date as date
    from `prod-analytics-1312.common.dimensions_date_dim`
    where cal_date >= '2018-01-01'
    and cal_date < current_date
), actual_churn as (
    select
        ou_id
        ,first_activation_date
        ,churn_date
    from `prod-analytics-1312.dbt_analytics.sa_summary`
    where churn_date is not null
        and churn_date >= '2020-01-01'
), merged as (
    select
        safe_cast(ou_id as int64) as ou_id
        ,churn_date as date
        ,'Churn' as change_type
    from actual_churn
    union all
    select
        ou_id
        ,date
        ,'Downsell' as change_type
    from mrr_change
    where mrr_downsell = 1
    union all
    select
        ou_id
        ,date
        ,'Upsell' as change_type
    from mrr_change
    where mrr_upsell = 1
), relevant_days_prelim as (
    select
        merged.ou_id
        ,merged.change_type
        ,merged.date as change_date
        ,days.date as leadup_date
    from merged
    cross join days 
        where days.date <= date_add(merged.date, interval -1 day)
        and days.date >= date_add(merged.date, interval -30 day)
), dupes as (
    select
        ou_id
        ,leadup_date
        ,count(1) as volume
    from relevant_days_prelim
    group by 1,2
    having volume > 1
), relevant_days as (
    select
        relevant_days_prelim.*
    from relevant_days_prelim
    left outer join dupes on dupes.ou_id = relevant_days_prelim.ou_id and dupes.leadup_date = relevant_days_prelim.leadup_date
    where dupes.ou_id is null
), daily_data as (
    select
         ifnull(relevant_days.change_type, 'Normal') as change_type
        ,relevant_days.change_date
        ,daily.date
        ,daily.dealer_id
        ,daily.days_since_activation
        ,daily.active_product_purchased
        ,daily.num_ads_purchased
        ,daily.pct_ads
        ,daily.sf_corrected_dealership_type
        ,daily.sf_dealer_cohorting
        ,daily.is_var
        ,daily.var_name
        ,daily.is_delinquent
        ,daily.salesforce_lot_size        
        ,daily.total_views
        ,daily.engages
        ,daily.calls
        ,daily.boards
        ,daily.dealer_feed_posted
        ,daily.manual_posted
        ,daily.alp
        ,daily.feed_active
        ,daily.manual_active
        ,daily.response_rate_excl_sb
        ,daily.med_response_delay_8to8
        ,daily.mrr
        ,daily.mrr_disc
        ,daily.value_total_w_floor
        ,daily.bumps_received
    from `prod-analytics-1312.dbt_analytics.dr_daily_detail` as daily
    left outer join relevant_days on relevant_days.ou_id = daily.dealer_id and relevant_days.leadup_date = daily.date
    where daily.date >= '2019-12-01'
), data_30d_lag as (
    select
         daily_data.change_type
        ,daily_data.change_date
        ,daily_data.date
        ,daily_data.dealer_id
        ,daily_data.days_since_activation
        ,daily_data.active_product_purchased
        ,daily_data.num_ads_purchased
        ,daily_data.pct_ads
        ,daily_data.sf_corrected_dealership_type
        ,daily_data.sf_dealer_cohorting
        ,daily_data.is_var
        ,daily_data.var_name
        ,daily_data.is_delinquent
        ,daily_data.salesforce_lot_size        
        ,sum(d30_data.total_views) as views
        ,sum(d30_data.engages) as engagements
        ,sum(d30_data.calls) as calls
        ,sum(d30_data.boards) as boards
        ,sum(d30_data.dealer_feed_posted) as feed_posted
        ,sum(d30_data.manual_posted) as manual_posted
        ,sum(d30_data.alp * (ifnull(d30_data.dealer_feed_posted,0) + ifnull(d30_data.manual_posted,0))) as alp_total
        ,sum(d30_data.feed_active) as feed_active
        ,sum(d30_data.manual_active) as manual_active
        ,sum(d30_data.response_rate_excl_sb * d30_data.engages) as response_rate_excl_sb_engagements
        ,avg(d30_data.med_response_delay_8to8) as med_response_delay_8to8_avg
        ,sum(d30_data.mrr) as mrr
        ,sum(d30_data.mrr_disc) as mrr_disc
        ,(sum(d30_data.mrr) / count(1)) / nullif((ifnull(sum(d30_data.engages),0) + ifnull(sum(d30_data.calls),0)),0) as cpe
        ,(1000 * sum(d30_data.mrr) / count(1)) / nullif((ifnull(sum(d30_data.total_views),0)),0) as cpm
        ,sum(d30_data.value_total_w_floor) as value_total_w_floor
        ,sum(d30_data.bumps_received) as bumps_received
        ,count(1) as days
    from daily_data
    cross join daily_data as d30_data
    where d30_data.dealer_id = daily_data.dealer_id
        and d30_data.date >= date_add(daily_data.date, interval -30 day)
        and d30_data.date <= date_add(daily_data.date, interval -1 day)
        and daily_data.date >= '2020-01-01'
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
)
    select
        change_type
        ,case 
            when alp_total / (ifnull(feed_posted,0) + ifnull(manual_posted,0)) > 30000 then 
                    round(alp_total / (ifnull(feed_posted,0) + ifnull(manual_posted,0))/5000)*5000
             else round(alp_total / (ifnull(feed_posted,0) + ifnull(manual_posted,0))/1000)*1000
             end as alp_rounded
        ,count(1) as outcomes
    from data_30d_lag
    group by 1,2