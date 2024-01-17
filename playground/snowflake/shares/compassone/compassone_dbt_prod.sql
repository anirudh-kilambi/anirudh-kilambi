-- dim customers has_volante_morrison_source = true 
drop view if exists compassone_dbt_prod_sharedb.sales_dimensions.dim_customers;

create or replace secure view compassone_dbt_prod_sharedb.sales_dimensions.dim_customers as 
(select * from dbt_prod.sales_dimensions.dim_customers where has_volante_us_source = true or has_t2e_source = true);

grant select on compassone_dbt_prod_sharedb.sales_dimensions.dim_customers to share compassone_dbt_prod;

-- dim_locations

drop view if exists compassone_dbt_prod_sharedb.sales_dimensions.dim_locations;

create or replace secure view compassone_dbt_prod_sharedb.sales_dimensions.dim_locations as 
(select * from dbt_prod.sales_dimensions.dim_locations WHERE data_source IN ('Volante US Data Source', 'T2E Data Source'));

grant select on compassone_dbt_prod_sharedb.sales_dimensions.dim_locations to share compassone_dbt_prod;

-- dim_locations_terminals_brands

drop view if exists compassone_dbt_prod_sharedb.sales_dimensions.dim_locations_terminals_brands;

create or replace secure view compassone_dbt_prod_sharedb.sales_dimensions.dim_locations_terminals_brands as 
(select * from dbt_prod.sales_dimensions.dim_locations_terminals_brands WHERE data_source IN ('Volante US Data Source', 'T2E Data Source'));

grant select on compassone_dbt_prod_sharedb.sales_dimensions.dim_locations_terminals_brands to share compassone_dbt_prod;

-- locations_volante_us_extensions

drop view if exists compassone_dbt_prod_sharedb.sales_extensions.locations_volante_us_extension;

create or replace secure view compassone_dbt_prod_sharedb.sales_extensions.locations_volante_us_extension as 
(select * from dbt_prod.sales_extensions.locations_volante_us_extension );

grant select on compassone_dbt_prod_sharedb.sales_extensions.locations_volante_us_extension to share compassone_dbt_prod;

-- orders_volante_us_extension

drop view if exists compassone_dbt_prod_sharedb.sales_extensions.orders_volante_us_extension;

create or replace secure view compassone_dbt_prod_sharedb.sales_extensions.orders_volante_us_extension as 
(select * from dbt_prod.sales_extensions.orders_volante_us_extension );

grant select on compassone_dbt_prod_sharedb.sales_extensions.orders_volante_us_extension to share compassone_dbt_prod;

-- order_details_volante_us_extension

drop view if exists compassone_dbt_prod_sharedb.sales_extensions.order_details_volante_us_extension;

create or replace secure view compassone_dbt_prod_sharedb.sales_extensions.order_details_volante_us_extension as 
(select * from dbt_prod.sales_extensions.order_details_volante_us_extension );

grant select on compassone_dbt_prod_sharedb.sales_extensions.order_details_volante_us_extension to share compassone_dbt_prod;

-- orders

drop view if exists compassone_dbt_prod_sharedb.sales_facts.orders;

create or replace secure view compassone_dbt_prod_sharedb.sales_facts.orders as 
(select * from dbt_prod.sales_facts.orders where data_source IN ('Volante US Data Source', 'T2E Data Source'));

grant select on compassone_dbt_prod_sharedb.sales_facts.orders to share compassone_dbt_prod;

-- order_details

drop view if exists compassone_dbt_prod_sharedb.sales_facts.order_details;

create or replace secure view compassone_dbt_prod_sharedb.sales_facts.order_details as 
(select * from dbt_prod.sales_facts.order_details where data_source IN ('Volante US Data Source', 'T2E Data Source'));

grant select on compassone_dbt_prod_sharedb.sales_facts.order_details to share compassone_dbt_prod;

-- order_discounts

drop view if exists compassone_dbt_prod_sharedb.sales_facts.order_discounts;

create or replace secure view compassone_dbt_prod_sharedb.sales_facts.order_discounts as 
(select * from dbt_prod.sales_facts.order_details where data_source IN ('Volante US Data Source', 'T2E Data Source'));

grant select on compassone_dbt_prod_sharedb.sales_facts.order_discounts to share compassone_dbt_prod;

select get_ddl('table', 'fidelity_shared_database.dbt_prod.order_discounts')


-- dim_units

drop view if exists compassone_dbt_prod_sharedb.sales_dimensions.dim_units;

create or replace secure view compassone_dbt_prod_sharedb.sales_dimensions.dim_units as 
(select * from dbt_prod.sales_dimensions.dim_units where sector_name = 'Morrison Healthcare Sector');

grant select on compassone_dbt_prod_sharedb.sales_dimensions.dim_units to share compassone_dbt_prod;

-- dim employees

drop view if exists compassone_dbt_prod_sharedb.sales_dimensions.dim_employees;

create or replace secure view compassone_dbt_prod_sharedb.sales_dimensions.dim_employees as 
(select * from dbt_prod.sales_dimensions.dim_employees where data_source IN ('Volante US Data Source', 'T2E Data Source'));

grant select on compassone_dbt_prod_sharedb.sales_dimensions.dim_employees to share compassone_dbt_prod;

-- dim menu_items

drop view if exists compassone_dbt_prod_sharedb.sales_dimensions.dim_menu_items;

create or replace secure view compassone_dbt_prod_sharedb.sales_dimensions.dim_menu_items as 
(select * from dbt_prod.sales_dimensions.dim_menu_items where data_source IN ('Volante US Data Source', 'T2E Data Source'));

grant select on compassone_dbt_prod_sharedb.sales_dimensions.dim_menu_items to share compassone_dbt_prod;

-- order_tenders
drop view if exists compassone_dbt_prod_sharedb.sales_facts.order_tenders;

create or replace secure view compassone_dbt_prod_sharedb.sales_facts.order_tenders as 
(select * from dbt_prod.sales_facts.order_tenders WHERE data_source IN ('Volante US Data Source', 'T2E Data Source'));

grant select on compassone_dbt_prod_sharedb.sales_facts.order_tenders to share compassone_dbt_prod;

-- dim order tenders extension

drop view if exists compassone_dbt_prod_sharedb.sales_extensions.order_tenders_extension;

create or replace secure view compassone_dbt_prod_sharedb.sales_extensions.order_tenders_extension as 
(select * from dbt_prod.sales_extensions.order_tenders_extension WHERE data_source IN ('Volante US Data Source', 'T2E Data Source'));

grant select on compassone_dbt_prod_sharedb.sales_extensions.order_tenders_extension to share compassone_dbt_prod;

DIM_UNITS (where sector_name = 'Morrison Healthcare Sector') 

WHERE data_source IN ('Volante US Data Source', 'T2E Data Source')
DIM_EMPLOYEES
DIM_MENU_ITEMS
ORDER_TENDERS_EXTENSION
ORDER_TENDERS

-- CREATE DIGITAL LEARNING VIEWS.
use schema digital_learning;

create or replace secure view ASSIGNED_CERTIFICATIONS as (select * from dbt_dev.digital_learning.ASSIGNED_CERTIFICATIONS);
create or replace secure view ASSIGNED_CONTENT as (select * from dbt_dev.digital_learning.assigned_content);
create or replace secure view ASSIGNED_COURSES as (select * from dbt_dev.digital_learning.assigned_courses);
create or replace secure view ASSIGNED_PROGRAMS as (select * from dbt_dev.digital_learning.assigned_PROGRAMS);
create or replace secure view certifications as (select * from dbt_dev.digital_learning.certifications);
create or replace secure view certification_learners as (select * from dbt_dev.digital_learning.certification_learners);
create or replace secure view courses as (select * from dbt_dev.digital_learning.courses);
create or replace secure view course_learners as (select * from dbt_dev.digital_learning.course_learners);
create or replace secure view learners as (select * from dbt_dev.digital_learning.learners);
create or replace secure view programs as (select * from dbt_dev.digital_learning.programs);
create or replace secure view program_learners as (select * from dbt_dev.digital_learning.program_learners);

grant select on view ASSIGNED_CERTIFICATIONS to share CD_COMPASSONE_DBT_DEV_OUTBOUND;
grant select on view assigned_content to share CD_COMPASSONE_DBT_DEV_OUTBOUND;
grant select on view assigned_courses to share CD_COMPASSONE_DBT_DEV_OUTBOUND;
grant select on view assigned_PROGRAMS to share CD_COMPASSONE_DBT_DEV_OUTBOUND;
grant select on view certifications to share CD_COMPASSONE_DBT_DEV_OUTBOUND;
grant select on view certification_learners to share CD_COMPASSONE_DBT_DEV_OUTBOUND;
grant select on view courses to share CD_COMPASSONE_DBT_DEV_OUTBOUND;
grant select on view course_learners to share CD_COMPASSONE_DBT_DEV_OUTBOUND;
grant select on view learners to share CD_COMPASSONE_DBT_DEV_OUTBOUND;
grant select on view programs to share CD_COMPASSONE_DBT_DEV_OUTBOUND;
grant select on view program_learners to share CD_COMPASSONE_DBT_DEV_OUTBOUND;


