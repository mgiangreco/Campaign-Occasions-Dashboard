
--Create a volatile table that links page_view_id to the associated page_campaign
CREATE volatile table clickstream as 
(select page_view_id, page_campaign FROM user_groupondw.fact_clickstream
where event_date= '2015-07-31' and bot_ind = 0
group by 1,2)
with data primary index (page_view_id)
on commit preserve rows;

-- Create a volatile table with page view info, including the page_campaign associated with each page_view
CREATE volatile table page_info as
(select pe.page_view_id, pe.page_type_name, pe.browse_header, c.page_campaign 
FROM sandbox.fc_page_extract_h pe
join clickstream c on c.page_view_id = pe.page_view_id 
where pe.event_date ='2015-07-31'
group by 1,2,3,4)
with data primary index (page_view_id)
on commit preserve rows;

--Create volatile table with order information, aggregated by parent_page_view_id
CREATE volatile table dealview_orders as 
(select parent_page_view_id, order_id, deal_key FROM sandbox.gliu_dps_raw 
where event_date = '2015-07-31' and order_id is not null)
with data primary index (order_id)
on commit preserve rows;


--Create an intermediate table with deal impressions, plus the associated page_campaign and browse_header
CREATE TABLE sandbox.mg_campaign_details as (
select wc.log_date, wc.content_name, wc.page_id, dv.page_type_name, dv.browse_header, dv.page_campaign, o.order_id, fom.gross_bookings, fom.gross_revenue, fom.net_operational_bookings, fom.net_operational_revenue, fom.units_sold 
FROM user_groupondw.bld_widget_contents wc
join page_info dv on dv.page_view_id = wc.page_id
join user_groupondw.dim_deal dd on dd.permalink = wc.content_name
left join dealview_orders o on o.parent_page_view_id=wc.page_id and o.deal_key=dd.deal_key
left join user_groupondw.fact_order_master fom on fom.order_id=o.order_id
where wc.content_type = 'deal' and wc.log_date = '2015-07-31') 
with data primary index (page_id) 
PARTITION BY RANGE_N(log_date  BETWEEN DATE '2014-01-01' AND DATE '2015-12-31' EACH INTERVAL '1' DAY , NO RANGE OR UNKNOWN);

--Total up deal impressions and orders for every deal in the campaign_details table from one day
CREATE VOLATILE TABLE dealimpress as 
(select log_date, content_name, browse_header, page_campaign, SUM(gross_bookings) as GB, SUM(gross_revenue) as GR, SUM(net_operational_bookings) as NOB, SUM(net_operational_revenue) as NOR, count(distinct page_id) as deal_impressions, count(distinct order_id) as orders
from sandbox.mg_campaign_details
where log_date= '2015-07-31'
group by 2,1,3,4)
with data primary index (content_name)
on commit preserve rows;

--Add PDS and supply_channel info onto the dealimpress table and put that in a new table
CREATE TABLE sandbox.mg_dealimpress as (
select wc.log_date, wc.content_name, dda.txnmy_pds_v3_name, wc.browse_header, wc.page_campaign, wc.GB, wc.GR, wc.NOB, wc.NOR, wc.deal_impressions, wc.orders FROM dealimpress wc
join user_groupondw.dim_deal dd on dd.permalink = wc.content_name
join user_groupondw.dim_deal_attr dda on dda.deal_key = dd.deal_key
group by 2,1,3,4,5,6,7,8,9,10,11)
with data primary index (content_name);


--Repeat everything above for all prior days in 2015
CREATE volatile table clickstream as 
(select page_view_id, page_campaign FROM user_groupondw.fact_clickstream
where event_date= '2015-07-30' and bot_ind = 0
)
with data primary index (page_view_id)
on commit preserve rows;

CREATE volatile table page_info as
(select pe.page_view_id, pe.page_type_name, pe.browse_header, c.page_campaign FROM sandbox.fc_page_extract_h pe
join clickstream c on c.page_view_id = pe.page_view_id 
where pe.event_date = '2015-07-30'
)
with data primary index (page_view_id) on commit preserve rows;

CREATE volatile table dealview_orders as 
(select parent_page_view_id, order_id, deal_key FROM sandbox.gliu_dps_raw 
where event_date = '2015-07-30' and order_id is not null)
with data primary index (order_id)
on commit preserve rows;

insert into sandbox.mg_campaign_details
select wc.log_date, wc.content_name, wc.page_id, dv.page_type_name, dv.browse_header, dv.page_campaign, o.order_id, fom.gross_bookings, fom.gross_revenue, fom.net_operational_bookings, fom.net_operational_revenue, fom.units_sold 
FROM user_groupondw.bld_widget_contents wc
join page_info dv on dv.page_view_id = wc.page_id
join user_groupondw.dim_deal dd on dd.permalink = wc.content_name
left join dealview_orders o on o.parent_page_view_id=wc.page_id and o.deal_key=dd.deal_key
left join user_groupondw.fact_order_master fom on fom.order_id=o.order_id
where wc.content_type = 'deal' and wc.log_date = '2015-07-30';

CREATE VOLATILE TABLE dealimpress as 
(select log_date, content_name, browse_header, page_campaign, SUM(gross_bookings) as GB, SUM(gross_revenue) as GR, SUM(net_operational_bookings) as NOB, SUM(net_operational_revenue) as NOR, count(distinct page_id) as deal_impressions, count(distinct order_id) as orders
from sandbox.mg_campaign_details
where log_date= '2015-07-30'
group by 2,1,3,4)
with data primary index (content_name)
on commit preserve rows;

insert into sandbox.mg_dealimpress
select wc.log_date, wc.content_name, dda.txnmy_pds_v3_name, wc.browse_header, wc.page_campaign, wc.GB, wc.GR, wc.NOB, wc.NOR, wc.deal_impressions, wc.orders FROM dealimpress wc
join user_groupondw.dim_deal dd on dd.permalink = wc.content_name
join user_groupondw.dim_deal_attr dda on dda.deal_key = dd.deal_key;

--ETC...

--Repeat for Sept 1-Dec 31, 2014, this time using the sandbox.fc_page_extract_2014 table instead of sandbox.fc_page_extract_h (August is not included because the sandbox.gliu_dps_raw begins in the middle of August 2014) 
CREATE volatile table clickstream as 
(select page_view_id, page_campaign FROM user_groupondw.fact_clickstream
where event_date= '2014-12-31' and bot_ind = 0
)
with data primary index (page_view_id)
on commit preserve rows;

CREATE volatile table page_info as
(select pe.page_view_id, pe.page_type_name, pe.browse_header, c.page_campaign FROM sandbox.fc_page_extract_2014 pe
join clickstream c on c.page_view_id = pe.page_view_id 
where pe.event_date = '2014-12-31'
)
with data primary index (page_view_id) on commit preserve rows;

CREATE volatile table dealview_orders as 
(select parent_page_view_id, order_id, deal_key FROM sandbox.gliu_dps_raw 
where event_date = '2014-12-31' and order_id is not null)
with data primary index (order_id)
on commit preserve rows;

insert into sandbox.mg_campaign_details
select wc.log_date, wc.content_name, wc.page_id, dv.page_type_name, dv.browse_header, dv.page_campaign, o.order_id, fom.gross_bookings, fom.gross_revenue, fom.net_operational_bookings, fom.net_operational_revenue, fom.units_sold 
FROM user_groupondw.bld_widget_contents wc
join page_info dv on dv.page_view_id = wc.page_id
join user_groupondw.dim_deal dd on dd.permalink = wc.content_name
left join dealview_orders o on o.parent_page_view_id=wc.page_id and o.deal_key=dd.deal_key
left join user_groupondw.fact_order_master fom on fom.order_id=o.order_id
where wc.content_type = 'deal' and wc.log_date = '2014-12-31';

CREATE VOLATILE TABLE dealimpress as 
(select log_date, content_name, browse_header, page_campaign, SUM(gross_bookings) as GB, SUM(gross_revenue) as GR, SUM(net_operational_bookings) as NOB, SUM(net_operational_revenue) as NOR, count(distinct page_id) as deal_impressions, count(distinct order_id) as orders
from sandbox.mg_campaign_details
where log_date= '2014-12-31'
group by 2,1,3,4)
with data primary index (content_name)
on commit preserve rows;

insert into sandbox.mg_dealimpress
select wc.log_date, wc.content_name, dda.txnmy_pds_v3_name, wc.browse_header, wc.page_campaign, wc.GB, wc.GR, wc.NOB, wc.NOR, wc.deal_impressions, wc.orders FROM dealimpress wc
join user_groupondw.dim_deal dd on dd.permalink = wc.content_name
join user_groupondw.dim_deal_attr dda on dda.deal_key = dd.deal_key;

--delete anomalous data from sandbox.mg_dealimpress table
DELETE FROM sandbox.mg_dealimpress
where browse_header='query-search'
AND page_campaign is not null;

drop table sandbox.mg_campaign_details;


--Create copy of mg_dealimpress table
create table sandbox.mg_campaign_dashboard as sandbox.mg_dealimpress with data and stats;

grant all on sandbox.mg_campaign_dashboard to public with grant option;

--Add new column to duplicate table
alter table sandbox.mg_campaign_dashboard
ADD page_campaign_count integer;

--Convert null page_campaign values to 'NO CAMPAIGN'' for every month (except August, which is not included in the 11-month timeframe we selected arbitrarily for this data set) 
UPDATE sandbox.mg_campaign_dashboard
SET page_campaign = 'NO CAMPAIGN'
where page_campaign is null
AND month(sandbox.mg_campaign_dashboard.log_date)=1;

UPDATE sandbox.mg_campaign_dashboard
SET page_campaign = 'NO CAMPAIGN'
where page_campaign is null
AND month(sandbox.mg_campaign_dashboard.log_date)=2;

UPDATE sandbox.mg_campaign_dashboard
SET page_campaign = 'NO CAMPAIGN'
where page_campaign is null
AND month(sandbox.mg_campaign_dashboard.log_date)=3;

UPDATE sandbox.mg_campaign_dashboard
SET page_campaign = 'NO CAMPAIGN'
where page_campaign is null
AND month(sandbox.mg_campaign_dashboard.log_date)=4;

UPDATE sandbox.mg_campaign_dashboard
SET page_campaign = 'NO CAMPAIGN'
where page_campaign is null
AND month(sandbox.mg_campaign_dashboard.log_date)=5;

UPDATE sandbox.mg_campaign_dashboard
SET page_campaign = 'NO CAMPAIGN'
where page_campaign is null
AND month(sandbox.mg_campaign_dashboard.log_date)=6;

UPDATE sandbox.mg_campaign_dashboard
SET page_campaign = 'NO CAMPAIGN'
where page_campaign is null
AND month(sandbox.mg_campaign_dashboard.log_date)=7;

UPDATE sandbox.mg_campaign_dashboard
SET page_campaign = 'NO CAMPAIGN'
where page_campaign is null
AND month(sandbox.mg_campaign_dashboard.log_date)=9;

UPDATE sandbox.mg_campaign_dashboard
SET page_campaign = 'NO CAMPAIGN'
where page_campaign is null
AND month(sandbox.mg_campaign_dashboard.log_date)=10;

UPDATE sandbox.mg_campaign_dashboard
SET page_campaign = 'NO CAMPAIGN'
where page_campaign is null
AND month(sandbox.mg_campaign_dashboard.log_date)=11;

UPDATE sandbox.mg_campaign_dashboard
SET page_campaign = 'NO CAMPAIGN'
where page_campaign is null
AND month(sandbox.mg_campaign_dashboard.log_date)=12;

--Add counts of page_campaigns to the new column
CREATE TABLE sandbox.mg_temp AS
(select log_date, content_name, count(distinct page_campaign) as count_
				from sandbox.mg_campaign_dashboard
				group by 1,2)
with data primary index (content_name);

--Populate the new column with actual page counts 
UPDATE sandbox.mg_campaign_dashboard

SET  page_campaign_count = sandbox.mg_temp.count_ 

where 
sandbox.mg_temp.log_date=sandbox.mg_campaign_dashboard.log_date
AND sandbox.mg_temp.content_name=sandbox.mg_campaign_dashboard.content_name
AND month(sandbox.mg_temp.log_date)=1

;


UPDATE sandbox.mg_campaign_dashboard

SET  page_campaign_count = sandbox.mg_temp.count_ 

where 
sandbox.mg_temp.log_date=sandbox.mg_campaign_dashboard.log_date
AND sandbox.mg_temp.content_name=sandbox.mg_campaign_dashboard.content_name
AND month(sandbox.mg_temp.log_date)=2

;

UPDATE sandbox.mg_campaign_dashboard

SET  page_campaign_count = sandbox.mg_temp.count_ 

where 
sandbox.mg_temp.log_date=sandbox.mg_campaign_dashboard.log_date
AND sandbox.mg_temp.content_name=sandbox.mg_campaign_dashboard.content_name
AND month(sandbox.mg_temp.log_date)=3

;

UPDATE sandbox.mg_campaign_dashboard

SET  page_campaign_count = sandbox.mg_temp.count_ 

where 
sandbox.mg_temp.log_date=sandbox.mg_campaign_dashboard.log_date
AND sandbox.mg_temp.content_name=sandbox.mg_campaign_dashboard.content_name
AND month(sandbox.mg_temp.log_date)=4

;

UPDATE sandbox.mg_campaign_dashboard

SET  page_campaign_count = sandbox.mg_temp.count_ 

where 
sandbox.mg_temp.log_date=sandbox.mg_campaign_dashboard.log_date
AND sandbox.mg_temp.content_name=sandbox.mg_campaign_dashboard.content_name
AND month(sandbox.mg_temp.log_date)=5

;


UPDATE sandbox.mg_campaign_dashboard

SET  page_campaign_count = sandbox.mg_temp.count_ 

where 
sandbox.mg_temp.log_date=sandbox.mg_campaign_dashboard.log_date
AND sandbox.mg_temp.content_name=sandbox.mg_campaign_dashboard.content_name
AND month(sandbox.mg_temp.log_date)=6

;


UPDATE sandbox.mg_campaign_dashboard

SET  page_campaign_count = sandbox.mg_temp.count_ 

where 
sandbox.mg_temp.log_date=sandbox.mg_campaign_dashboard.log_date
AND sandbox.mg_temp.content_name=sandbox.mg_campaign_dashboard.content_name
AND month(sandbox.mg_temp.log_date)=7

;


UPDATE sandbox.mg_campaign_dashboard

SET  page_campaign_count = sandbox.mg_temp.count_ 

where 
sandbox.mg_temp.log_date=sandbox.mg_campaign_dashboard.log_date
AND sandbox.mg_temp.content_name=sandbox.mg_campaign_dashboard.content_name
AND month(sandbox.mg_temp.log_date)=9

;


UPDATE sandbox.mg_campaign_dashboard

SET  page_campaign_count = sandbox.mg_temp.count_ 

where 
sandbox.mg_temp.log_date=sandbox.mg_campaign_dashboard.log_date
AND sandbox.mg_temp.content_name=sandbox.mg_campaign_dashboard.content_name
AND month(sandbox.mg_temp.log_date)=10

;


UPDATE sandbox.mg_campaign_dashboard

SET  page_campaign_count = sandbox.mg_temp.count_ 

where 
sandbox.mg_temp.log_date=sandbox.mg_campaign_dashboard.log_date
AND sandbox.mg_temp.content_name=sandbox.mg_campaign_dashboard.content_name
AND month(sandbox.mg_temp.log_date)=11

;


UPDATE sandbox.mg_campaign_dashboard

SET  page_campaign_count = sandbox.mg_temp.count_ 

where 
sandbox.mg_temp.log_date=sandbox.mg_campaign_dashboard.log_date
AND sandbox.mg_temp.content_name=sandbox.mg_campaign_dashboard.content_name
AND month(sandbox.mg_temp.log_date)=12

;

--delete all rows where page_campaign_count is 1 and page_campaign is 'NO CAMPAIGN'
DELETE FROM sandbox.mg_campaign_dashboard
WHERE page_campaign_count=1
AND page_campaign='NO CAMPAIGN';

--Create a new table with "in campaign" and "out campaign" metrics
CREATE TABLE sandbox.mg_occasions_dash as (
  
  select page_campaign, content_name, txnmy_pds_v3_name, log_date, browse_header,
  deal_impressions as in_campaign_deal_impressions, 
  (my_deal_impressions - deal_impressions) as out_campaign_impressions, 
  my_deal_impressions as total_deal_impressions,
  
  orders as in_campaign_orders,
  (my_orders - orders) as out_campaign_orders,
  my_orders as total_orders,
  
  GB as in_campaign_GB,
  (my_GB - GB) as out_campaign_GB,
  my_GB as total_GB,
  
  GR as in_campaign_GR,
  (my_GR - GR) as out_campaign_GR,
  my_GR as total_GR,
  
  NOB as in_campaign_NOB,
  (my_NOB - NOB) as out_campaign_NOB,
  my_NOB as total_NOB,
  
  NOR as in_campaign_NOR,
  (my_NOR - NOR) as out_campaign_NOR,
  my_NOR as total_NOR
  

from
(
select 
a.*, 
sum(deal_impressions) over (partition by log_date, content_name) as my_deal_impressions,

sum(orders) over (partition by log_date, content_name) as my_orders,

sum(GB) over (partition by log_date, content_name) as my_GB,

sum(GR) over (partition by log_date, content_name) as my_GR,

sum(NOB) over (partition by log_date, content_name) as my_NOB,

sum(NOR) over (partition by log_date, content_name) as my_NOR

from 
sandbox.mg_campaign_dashboard a
where month(log_date) = 1 ) a)
with data primary index (page_campaign, log_date, content_name)
PARTITION BY RANGE_N(log_date  BETWEEN DATE '2014-01-01' AND DATE '2015-12-31' EACH INTERVAL '1' DAY , NO RANGE OR UNKNOWN);


INSERT INTO sandbox.mg_occasions_dash 
  
  select page_campaign, content_name, txnmy_pds_v3_name, log_date, browse_header,
  deal_impressions as in_campaign_deal_impressions, 
  (my_deal_impressions - deal_impressions) as out_campaign_impressions, 
  my_deal_impressions as total_deal_impressions,
  
  orders as in_campaign_orders,
  (my_orders - orders) as out_campaign_orders,
  my_orders as total_orders,
  
  GB as in_campaign_GB,
  (my_GB - GB) as out_campaign_GB,
  my_GB as total_GB,
  
  GR as in_campaign_GR,
  (my_GR - GR) as out_campaign_GR,
  my_GR as total_GR,
  
  NOB as in_campaign_NOB,
  (my_NOB - NOB) as out_campaign_NOB,
  my_NOB as total_NOB,
  
  NOR as in_campaign_NOR,
  (my_NOR - NOR) as out_campaign_NOR,
  my_NOR as total_NOR
  

from
(
select 
a.*, 
sum(deal_impressions) over (partition by log_date, content_name) as my_deal_impressions,

sum(orders) over (partition by log_date, content_name) as my_orders,

sum(GB) over (partition by log_date, content_name) as my_GB,

sum(GR) over (partition by log_date, content_name) as my_GR,

sum(NOB) over (partition by log_date, content_name) as my_NOB,

sum(NOR) over (partition by log_date, content_name) as my_NOR


from 
sandbox.mg_campaign_dashboard a
where month(log_date) = 2) a
;


INSERT INTO sandbox.mg_occasions_dash 
  
  select page_campaign, content_name, txnmy_pds_v3_name, log_date, browse_header,
  deal_impressions as in_campaign_deal_impressions, 
  (my_deal_impressions - deal_impressions) as out_campaign_impressions, 
  my_deal_impressions as total_deal_impressions,
  
  orders as in_campaign_orders,
  (my_orders - orders) as out_campaign_orders,
  my_orders as total_orders,
  
  GB as in_campaign_GB,
  (my_GB - GB) as out_campaign_GB,
  my_GB as total_GB,
  
  GR as in_campaign_GR,
  (my_GR - GR) as out_campaign_GR,
  my_GR as total_GR,
  
  NOB as in_campaign_NOB,
  (my_NOB - NOB) as out_campaign_NOB,
  my_NOB as total_NOB,
  
  NOR as in_campaign_NOR,
  (my_NOR - NOR) as out_campaign_NOR,
  my_NOR as total_NOR
  

from
(
select 
a.*, 
sum(deal_impressions) over (partition by log_date, content_name) as my_deal_impressions,

sum(orders) over (partition by log_date, content_name) as my_orders,

sum(GB) over (partition by log_date, content_name) as my_GB,

sum(GR) over (partition by log_date, content_name) as my_GR,

sum(NOB) over (partition by log_date, content_name) as my_NOB,

sum(NOR) over (partition by log_date, content_name) as my_NOR

from 
sandbox.mg_campaign_dashboard a
where month(log_date) = 3) a
;

INSERT INTO sandbox.mg_occasions_dash 
  
  select page_campaign, content_name, txnmy_pds_v3_name, log_date, browse_header,
  deal_impressions as in_campaign_deal_impressions, 
  (my_deal_impressions - deal_impressions) as out_campaign_impressions, 
  my_deal_impressions as total_deal_impressions,
  
  orders as in_campaign_orders,
  (my_orders - orders) as out_campaign_orders,
  my_orders as total_orders,
  
  GB as in_campaign_GB,
  (my_GB - GB) as out_campaign_GB,
  my_GB as total_GB,
  
  GR as in_campaign_GR,
  (my_GR - GR) as out_campaign_GR,
  my_GR as total_GR,
  
  NOB as in_campaign_NOB,
  (my_NOB - NOB) as out_campaign_NOB,
  my_NOB as total_NOB,
  
  NOR as in_campaign_NOR,
  (my_NOR - NOR) as out_campaign_NOR,
  my_NOR as total_NOR
  

from
(
select 
a.*, 
sum(deal_impressions) over (partition by log_date, content_name) as my_deal_impressions,

sum(orders) over (partition by log_date, content_name) as my_orders,

sum(GB) over (partition by log_date, content_name) as my_GB,

sum(GR) over (partition by log_date, content_name) as my_GR,

sum(NOB) over (partition by log_date, content_name) as my_NOB,

sum(NOR) over (partition by log_date, content_name) as my_NOR

from 
sandbox.mg_campaign_dashboard a
where month(log_date) = 4) a
;

INSERT INTO sandbox.mg_occasions_dash 
  
  select page_campaign, content_name, txnmy_pds_v3_name, log_date, browse_header,
  deal_impressions as in_campaign_deal_impressions, 
  (my_deal_impressions - deal_impressions) as out_campaign_impressions, 
  my_deal_impressions as total_deal_impressions,
  
  orders as in_campaign_orders,
  (my_orders - orders) as out_campaign_orders,
  my_orders as total_orders,
  
  GB as in_campaign_GB,
  (my_GB - GB) as out_campaign_GB,
  my_GB as total_GB,
  
  GR as in_campaign_GR,
  (my_GR - GR) as out_campaign_GR,
  my_GR as total_GR,
  
  NOB as in_campaign_NOB,
  (my_NOB - NOB) as out_campaign_NOB,
  my_NOB as total_NOB,
  
  NOR as in_campaign_NOR,
  (my_NOR - NOR) as out_campaign_NOR,
  my_NOR as total_NOR
  

from
(
select 
a.*, 
sum(deal_impressions) over (partition by log_date, content_name) as my_deal_impressions,

sum(orders) over (partition by log_date, content_name) as my_orders,

sum(GB) over (partition by log_date, content_name) as my_GB,

sum(GR) over (partition by log_date, content_name) as my_GR,

sum(NOB) over (partition by log_date, content_name) as my_NOB,

sum(NOR) over (partition by log_date, content_name) as my_NOR

from 
sandbox.mg_campaign_dashboard a
where month(log_date) = 5) a
;

INSERT INTO sandbox.mg_occasions_dash 
  
  select page_campaign, content_name, txnmy_pds_v3_name, log_date, browse_header,
  deal_impressions as in_campaign_deal_impressions, 
  (my_deal_impressions - deal_impressions) as out_campaign_impressions, 
  my_deal_impressions as total_deal_impressions,
  
  orders as in_campaign_orders,
  (my_orders - orders) as out_campaign_orders,
  my_orders as total_orders,
  
  GB as in_campaign_GB,
  (my_GB - GB) as out_campaign_GB,
  my_GB as total_GB,
  
  GR as in_campaign_GR,
  (my_GR - GR) as out_campaign_GR,
  my_GR as total_GR,
  
  NOB as in_campaign_NOB,
  (my_NOB - NOB) as out_campaign_NOB,
  my_NOB as total_NOB,
  
  NOR as in_campaign_NOR,
  (my_NOR - NOR) as out_campaign_NOR,
  my_NOR as total_NOR
  

from
(
select 
a.*, 
sum(deal_impressions) over (partition by log_date, content_name) as my_deal_impressions,

sum(orders) over (partition by log_date, content_name) as my_orders,

sum(GB) over (partition by log_date, content_name) as my_GB,

sum(GR) over (partition by log_date, content_name) as my_GR,

sum(NOB) over (partition by log_date, content_name) as my_NOB,

sum(NOR) over (partition by log_date, content_name) as my_NOR

from 
sandbox.mg_campaign_dashboard a
where month(log_date) = 6) a
;


INSERT INTO sandbox.mg_occasions_dash 
  
  select page_campaign, content_name, txnmy_pds_v3_name, log_date, browse_header,
  deal_impressions as in_campaign_deal_impressions, 
  (my_deal_impressions - deal_impressions) as out_campaign_impressions, 
  my_deal_impressions as total_deal_impressions,
  
  orders as in_campaign_orders,
  (my_orders - orders) as out_campaign_orders,
  my_orders as total_orders,
  
  GB as in_campaign_GB,
  (my_GB - GB) as out_campaign_GB,
  my_GB as total_GB,
  
  GR as in_campaign_GR,
  (my_GR - GR) as out_campaign_GR,
  my_GR as total_GR,
  
  NOB as in_campaign_NOB,
  (my_NOB - NOB) as out_campaign_NOB,
  my_NOB as total_NOB,
  
  NOR as in_campaign_NOR,
  (my_NOR - NOR) as out_campaign_NOR,
  my_NOR as total_NOR
  

from
(
select 
a.*, 
sum(deal_impressions) over (partition by log_date, content_name) as my_deal_impressions,

sum(orders) over (partition by log_date, content_name) as my_orders,

sum(GB) over (partition by log_date, content_name) as my_GB,

sum(GR) over (partition by log_date, content_name) as my_GR,

sum(NOB) over (partition by log_date, content_name) as my_NOB,

sum(NOR) over (partition by log_date, content_name) as my_NOR

from 
sandbox.mg_campaign_dashboard a
where month(log_date) = 7) a
;


INSERT INTO sandbox.mg_occasions_dash 
  
  select page_campaign, content_name, txnmy_pds_v3_name, log_date, browse_header,
  deal_impressions as in_campaign_deal_impressions, 
  (my_deal_impressions - deal_impressions) as out_campaign_impressions, 
  my_deal_impressions as total_deal_impressions,
  
  orders as in_campaign_orders,
  (my_orders - orders) as out_campaign_orders,
  my_orders as total_orders,
  
  GB as in_campaign_GB,
  (my_GB - GB) as out_campaign_GB,
  my_GB as total_GB,
  
  GR as in_campaign_GR,
  (my_GR - GR) as out_campaign_GR,
  my_GR as total_GR,
  
  NOB as in_campaign_NOB,
  (my_NOB - NOB) as out_campaign_NOB,
  my_NOB as total_NOB,
  
  NOR as in_campaign_NOR,
  (my_NOR - NOR) as out_campaign_NOR,
  my_NOR as total_NOR
  

from
(
select 
a.*, 
sum(deal_impressions) over (partition by log_date, content_name) as my_deal_impressions,

sum(orders) over (partition by log_date, content_name) as my_orders,

sum(GB) over (partition by log_date, content_name) as my_GB,

sum(GR) over (partition by log_date, content_name) as my_GR,

sum(NOB) over (partition by log_date, content_name) as my_NOB,

sum(NOR) over (partition by log_date, content_name) as my_NOR

from 
sandbox.mg_campaign_dashboard a
where month(log_date) = 9) a
;


INSERT INTO sandbox.mg_occasions_dash 
  
  select page_campaign, content_name, txnmy_pds_v3_name, log_date, browse_header,
  deal_impressions as in_campaign_deal_impressions, 
  (my_deal_impressions - deal_impressions) as out_campaign_impressions, 
  my_deal_impressions as total_deal_impressions,
  
  orders as in_campaign_orders,
  (my_orders - orders) as out_campaign_orders,
  my_orders as total_orders,
  
  GB as in_campaign_GB,
  (my_GB - GB) as out_campaign_GB,
  my_GB as total_GB,
  
  GR as in_campaign_GR,
  (my_GR - GR) as out_campaign_GR,
  my_GR as total_GR,
  
  NOB as in_campaign_NOB,
  (my_NOB - NOB) as out_campaign_NOB,
  my_NOB as total_NOB,
  
  NOR as in_campaign_NOR,
  (my_NOR - NOR) as out_campaign_NOR,
  my_NOR as total_NOR
  

from
(
select 
a.*, 
sum(deal_impressions) over (partition by log_date, content_name) as my_deal_impressions,

sum(orders) over (partition by log_date, content_name) as my_orders,

sum(GB) over (partition by log_date, content_name) as my_GB,

sum(GR) over (partition by log_date, content_name) as my_GR,

sum(NOB) over (partition by log_date, content_name) as my_NOB,

sum(NOR) over (partition by log_date, content_name) as my_NOR

from 
sandbox.mg_campaign_dashboard a
where month(log_date) = 10) a
;


INSERT INTO sandbox.mg_occasions_dash 
  
  select page_campaign, content_name, txnmy_pds_v3_name, log_date, browse_header,
  deal_impressions as in_campaign_deal_impressions, 
  (my_deal_impressions - deal_impressions) as out_campaign_impressions, 
  my_deal_impressions as total_deal_impressions,
  
  orders as in_campaign_orders,
  (my_orders - orders) as out_campaign_orders,
  my_orders as total_orders,
  
  GB as in_campaign_GB,
  (my_GB - GB) as out_campaign_GB,
  my_GB as total_GB,
  
  GR as in_campaign_GR,
  (my_GR - GR) as out_campaign_GR,
  my_GR as total_GR,
  
  NOB as in_campaign_NOB,
  (my_NOB - NOB) as out_campaign_NOB,
  my_NOB as total_NOB,
  
  NOR as in_campaign_NOR,
  (my_NOR - NOR) as out_campaign_NOR,
  my_NOR as total_NOR
  

from
(
select 
a.*, 
sum(deal_impressions) over (partition by log_date, content_name) as my_deal_impressions,

sum(orders) over (partition by log_date, content_name) as my_orders,

sum(GB) over (partition by log_date, content_name) as my_GB,

sum(GR) over (partition by log_date, content_name) as my_GR,

sum(NOB) over (partition by log_date, content_name) as my_NOB,

sum(NOR) over (partition by log_date, content_name) as my_NOR

from 
sandbox.mg_campaign_dashboard a
where month(log_date) = 11) a
;

INSERT INTO sandbox.mg_occasions_dash 
  
  select page_campaign, content_name, txnmy_pds_v3_name, log_date, browse_header,
  deal_impressions as in_campaign_deal_impressions, 
  (my_deal_impressions - deal_impressions) as out_campaign_impressions, 
  my_deal_impressions as total_deal_impressions,
  
  orders as in_campaign_orders,
  (my_orders - orders) as out_campaign_orders,
  my_orders as total_orders,
  
  GB as in_campaign_GB,
  (my_GB - GB) as out_campaign_GB,
  my_GB as total_GB,
  
  GR as in_campaign_GR,
  (my_GR - GR) as out_campaign_GR,
  my_GR as total_GR,
  
  NOB as in_campaign_NOB,
  (my_NOB - NOB) as out_campaign_NOB,
  my_NOB as total_NOB,
  
  NOR as in_campaign_NOR,
  (my_NOR - NOR) as out_campaign_NOR,
  my_NOR as total_NOR
  

from
(
select 
a.*, 
sum(deal_impressions) over (partition by log_date, content_name) as my_deal_impressions,

sum(orders) over (partition by log_date, content_name) as my_orders,

sum(GB) over (partition by log_date, content_name) as my_GB,

sum(GR) over (partition by log_date, content_name) as my_GR,

sum(NOB) over (partition by log_date, content_name) as my_NOB,

sum(NOR) over (partition by log_date, content_name) as my_NOR

from 
sandbox.mg_campaign_dashboard a
where month(log_date) = 12) a
;


grant all on sandbox.mg_occasions_dash to public with grant option;



--for rows with out_campaign_orders only, set out campaign GB, GR, NOB, NOR equal to total GB, GR, NOB, NOR
UPDATE sandbox.mg_occasions_dash

SET  out_campaign_GB = total_GB

where out_campaign_orders is not null 
AND out_campaign_orders <> 0
AND out_campaign_GB is null;


UPDATE sandbox.mg_occasions_dash

SET  out_campaign_GR = total_GR

where out_campaign_orders is not null 
AND out_campaign_orders <> 0
AND out_campaign_GR is null;


UPDATE sandbox.mg_occasions_dash

SET  out_campaign_NOB = total_NOB

where out_campaign_orders is not null 
AND out_campaign_orders <> 0
AND out_campaign_NOB is null;


UPDATE sandbox.mg_occasions_dash

SET  out_campaign_NOR = total_NOR

where out_campaign_orders is not null 
AND out_campaign_orders <> 0
AND out_campaign_NOR is null;







