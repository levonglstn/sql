with buyers as (select 
    id, 
    user_id, 
    status, 
    cast(created_at as datetime) as created_at_dt,
    cast(captured_at as datetime) as captured_at_dt

from `psychic-kite-343608.shotgun.buyers_order_history`),

events AS (

SELECT
  id,
  event_id,
  user_id,
  CASE WHEN amount IS NULL AND status = 'captured' AND captured_at IS NOT NULL THEN 0
        WHEN amount is NULL AND status = 'captured' AND user_id is NULL AND captured_at IS NOT NULL THEN 0
        ELSE amount END AS amount_spend,
  item_count, 
  status,
  CAST(created_at AS datetime) AS created_at_dt,
  CAST(captured_at AS datetime) AS captured_at_dt

FROM `psychic-kite-343608.shotgun.event_orders`),

events_captured as (

  select *
  from events
  where status = 'captured' --- filter on only captured orders (completed)
),

cte1 as (

  select distinct 
			user_id
  from events_captured
  union all
  select 
			user_id
  from buyers
),

cte2 as (

  select 
			user_id, 
			count(*) AS id_count --- count of orders by user_id
  from cte1
  group by 1
),

event_segment_count as (

select 
  events_captured.event_id, 
  case  when cte2.id_count = 1 then 'new customer' --- if count = 1, it means it's his first order
        when cte2.id_count > 1 and cte2.id_count <= 3 then 'casual buyer'
        else 'power buyer' end as segment,
  count(*) AS count,
from events_captured
left join cte2 on events_captured.user_id = cte2.user_id
group by 1, 2
)

select *,
  round(count / sum(count) OVER (partition by event_id) * 100,2) AS pct
from event_segment_count
order by event_id asc
