-- fidelity_shared_database.dbt_prod.orders;
create or replace secure view FIDELITY_SHARED_DATABASE.DBT_PROD.ORDERS
as (
    select KEY,
    LOCATION_FK,
    LOCATION_TERMINALS_FK,
    CUSTOMER_FK,
    GRAIN_LVL,
    ORDER_ID,
    SALES_CHANNEL,
    IS_GUEST_CUSTOMER,
    ORDER_FISCAL_DATE_FK,
    ORDER_FISCAL_DATETIME_FK,
    PICKUP_FISCAL_DATETIME_FK,
    APP_NAME,
    ORDER_TYPE,
    DELIVERY_LOCATION,
    TRANSACTION_TYPE,
    EMPLOYEE_ID,
    EMPLOYEE_NAME,
    PAYMENT_METHOD,
    SUBTOTAL,
    SERVICE_FEE,
    DELIVERY_FEE,
    TIP,
    DISCOUNT,
    TOTAL_TAX,
    NET_SALES,
    TOTAL_SALES,
    ITEM_QUANTITY,
    EXCLUDE_USER,
    EXCLUDE_USER_REASON,
    CHECK_OUTLIER,
    DATA_SOURCE,
    ETL_EXTRACT_TIMESTAMP,
    INCREMENTAL_TIMESTAMP
    from dbt_prod.sales_facts.orders m 
    join (
        select distinct order_fk from dbt_prod.sales_extensions.orders_locations_extension
        where unit_id in (
            '21094', '21096', '21103', '21107', '21120', '21122', '21128', '21130', '23037', '24667', '43766', '50386', '50387', '60166', '60167', '60168', '60826', '60827', '60868'
        )
    ) as units
    on m.key = units.order_fk
);

-- FIDELITY_SHARED_DATABASE.DBT_PROD.ORDER_DISCOUNTS

create or replace secure view FIDELITY_SHARED_DATABASE.DBT_PROD.ORDER_DISCOUNTS
as (
    select KEY,
    m.ORDER_FK,
    DISCOUNT_TYPE,
    DISCOUNT_METHOD,
    DISCOUNT_VALUE,
    CODE,
    ORDER_DISCOUNT_AMOUNT,
    DATA_SOURCE,
    INCREMENTAL_TIMESTAMP
    from dbt_prod.sales_facts.order_discounts m 
    join (
        select distinct order_fk from dbt_prod.sales_extensions.orders_locations_extension
        where unit_id in (
            '21094', '21096', '21103', '21107', '21120', '21122', '21128', '21130', '23037', '24667', '43766', '50386', '50387', '60166', '60167', '60168', '60826', '60827', '60868'
        )
    ) as units
    on m.order_fk = units.order_fk
);

-- FIDELITY_SHARED_DATABASE.DBT_PROD.ORDER_TENDERS
create or replace secure view FIDELITY_SHARED_DATABASE.DBT_PROD.ORDER_TENDERS
as (
    select KEY,
    m.ORDER_FK,
    ORDER_ID,
    TENDER_TYPE,
    TENDER_SUBTYPE,
    TENDER_NAME,
    APPLIED_AMOUNT,
    MERCHANT,
    CASH_AND_CHEQUE_WAS_APPLIED,
    CASH_AND_CHEQUE_APPLIED_AMOUNT,
    CASH_AND_CHEQUE_TYPE,
    CASHLESS_PAY_WAS_APPLIED,
    CASHLESS_PAY_APPLIED_AMOUNT,
    CASHLESS_PAY_TYPE,
    CASHLESS_PAY_NAME,
    CREDIT_CARD_WAS_APPLIED,
    CREDIT_CARD_APPLIED_AMOUNT,
    CREDIT_CARD_TYPE,
    DIGITAL_WALLET_WAS_APPLIED,
    DIGITAL_WALLET_APPLIED_AMOUNT,
    DIGITAL_WALLET_TYPE,
    GIFT_CARD_WAS_APPLIED,
    GIFT_CARD_APPLIED_AMOUNT,
    GIFT_CARD_TYPE,
    MEAL_PLAN_WAS_APPLIED,
    MEAL_PLAN_APPLIED_AMOUNT,
    MEAL_PLAN_TYPE,
    MEAL_PLAN_NAME,
    MEAL_EXCHANGE_WAS_APPLIED,
    MEAL_EXCHANGE_APPLIED_AMOUNT,
    MEAL_SWIPES_WAS_APPLIED,
    MEAL_SWIPES_APPLIED_AMOUNT,
    MEAL_PLAN_DECLINING_BALANCE_WAS_APPLIED,
    MEAL_PLAN_DECLINING_BALANCE_APPLIED_AMOUNT,
    DATA_SOURCE,
    INCREMENTAL_TIMESTAMP,
    LOCATION_FK,
    LOCATION_TERMINALS_FK
    from dbt_prod.sales_facts.order_tenders m 
    join (
        select distinct order_fk from dbt_prod.sales_extensions.orders_locations_extension
        where unit_id in (
            '21094', '21096', '21103', '21107', '21120', '21122', '21128', '21130', '23037', '24667', '43766', '50386', '50387', '60166', '60167', '60168', '60826', '60827', '60868'
        )
    ) as units
    on m.order_fk = units.order_fk
);

GRANT SELECT ON VIEW ORDERS TO SHARE FIDELITY_DATA_SHARE;
GRANT SELECT ON VIEW ORDER_DISCOUNTS TO SHARE FIDELITY_DATA_SHARE;
GRANT SELECT ON VIEW ORDER_TENDERS TO SHARE FIDELITY_DATA_SHARE;
