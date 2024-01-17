with sales as (

select
     f.SKU_NUMBER,
   -- f.product_item_id,
    f.source_canteen_location_key market_fk,
    md5(f.source_canteen_location_key || f.SKU_NUMBER) market_sku_fk,
    sum(f.quantity_sold) qty_sold,
    sum(f.product_sales_amount) product_sales,
    count(distinct week(f.transaction_date)) weeks_in_period


from dbt_dev.reporting.canteen_transactions_parent f
    where
        TRANSACTION_DATE between '2023-07-01' and '2023-09-18'
        and f.DIVISION in ('Southeast Division', 'Northeast Division (Canteen Sector)')
group by all
  ),
  
market as (
select distinct operation_id,
                market_number,
                market_name,
                account_number,
                location_number,
                source_canteen_location_key,
                product_item_id,
                sku_number
from dbt_dev.reporting.canteen_transactions_parent
),

--this is what is on planograms and ever existed in sales table for what is on current planogram
planogram as (
  select   pd.operation_id,
         pd.account_id,
         pd.location_id,
         pd.delivery_location_id,
         pd.product_item_id,
         ml.name as location_name, 
         ml.location_number,
         a.name as account_name,
         a.account_number,
         dl.delivery_location_number as market_number,
         dl.name as market_name,
         m.source_canteen_location_key,
         m.sku_number
       --  sm.sku_number
         
       
from cgna_cdl_datatechnology_distilr_share.canteen.market_planogram_detail pd
inner join cgna_cdl_datatechnology_distilr_share.canteen.market_location ml on ml.operation_id = pd.operation_id and ml.location_id=pd.location_id and ml.account_id = pd.account_id
inner join cgna_cdl_datatechnology_distilr_share.canteen.market_account a on a.operation_id=pd.operation_id and pd.account_id=a.account_id
inner join cgna_cdl_datatechnology_distilr_share.canteen.market_delivery_location dl on pd.location_id=dl.location_id and pd.delivery_location_id = dl.delivery_location_id
inner join market m on pd.operation_id = m.operation_id and dl.delivery_location_number = m.market_number
                                                       and dl.name = m.market_name
                                                       and a.account_number = m.account_number
                                                       and ml.location_number = m.location_number
                                                       and pd.product_item_id = m.product_item_id
  )
  
  
  
  select COALESCE(s.SKU_NUMBER, p.sku_number) AS sku_number,
                  s.market_fk, 
                  s.market_sku_fk,
                  s.qty_sold,
                  s.product_sales,
                  s.weeks_in_period,
                  p.location_name, 
                  p.location_number,
                  p.account_name,
                  p.account_number,
                  p.market_number,
                  p.market_name,
                  p.source_canteen_location_key,
                  CASE WHEN p.sku_number IS NOT NULL THEN 'On Planogram' ELSE 'Not on Planogram' END AS planogram_status
FROM sales s
FULL OUTER JOIN planogram p ON s.market_fk = p.source_canteen_location_key AND s.SKU_NUMBER = p.sku_number
