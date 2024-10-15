{{ config(
    tags=['level-0-1']
)}}

select updated_at,grade,created_at,impression,last_name,id,first_name,age,dpt_opCode,txn_year from (
    select *, row_number() over (partition by id order by updated_at desc) as rank from
       (
           select updated_at,grade,created_at,impression,last_name,id,first_name,age,dpt_opCode,txn_year from adhoc.analytics.analytics_account_students where id not in (select distinct id from sms.stream.analytics_account_students where dpt_opCode = 'D')
           union all
           select updated_at,grade,created_at,impression,last_name,id,first_name,age,dpt_opCode,year(created_at) as txn_year from sms.stream.analytics_account_students where id not in (select distinct id from sms.stream.analytics_account_students where dpt_opCode = 'D')
       ) aggregate
) as ranked_aggregate
where rank = 1 and dpt_opCode <> 'D'
