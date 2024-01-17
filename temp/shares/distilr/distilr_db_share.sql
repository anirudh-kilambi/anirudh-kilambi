-- canteen
use database distilr_db_share;
create or replace secure view canteen.market_account as (select * from dataops_source.canteen.market_account);
create or replace secure view canteen.market_delivery_location as (select * from dataops_source.canteen.market_delivery_location);
create or replace secure view canteen.market_location as (select * from dataops_source.canteen.market_location);
create or replace secure view canteen.market_planogram_detail as (select * from dataops_source.canteen.market_planogram_detail);
create or replace secure view canteen.sharedservices_product_item as (select * from dataops_source.canteen.sharedservices_product_item);


grant select on view canteen.market_account to share distilr_share;
grant select on view canteen.market_delivery_location to share distilr_share;
grant select on view canteen.market_location to share distilr_share;
grant select on view canteen.market_planogram_detail to share distilr_share;
grant select on view canteen.sharedservices_product_item to share distilr_share;
