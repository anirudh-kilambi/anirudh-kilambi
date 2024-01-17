-- GRANTS
grant reference_usage on external_volume cgna_lake_iceberg to role dataops_source_read_role;

grant usage on integration cgna_mystaff_catalog to role dataops_source_read_role;


-- creating tables
USE DATABASE DATAOPS_SOURCE;

CREATE EXTERNAL VOLUME CGNA_LAKE_ICEBERG
  STORAGE_LOCATIONS =
  (
    (
    NAME = 'cgna-lake-iceberg'
    STORAGE_PROVIDER = 'S3'
    STORAGE_BASE_URL = 's3://cdl-dataops-iceberg-datalake/'
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::736888855894:role/cgna-snowflake_prod'
    )
  ); 

-- For every namespace in iceberg, create a catalog_integration in this format 
create or replace catalog integration cgna_{namespace}_catalog
catalog_source    = GLUE
catalog_namespace = '{namespace}' -- change this to whatever the namespace is in iceberg
table_format      = ICEBERG
glue_aws_role_arn = 'arn:aws:iam::736888855894:role/cgna-snowflake_prod'
glue_catalog_id   = '736888855894'
enabled           = true;

-- for each iceberg table
create or replace iceberg table {sfDB}.{sfSchema}.{sfTable}
external_volume = 'cgna_lake_iceberg'
catalog = '{cgna_{namespace}_catalog}'
catalog_table_name = '{iceberg table name}';

-- to refresh table after change in datalake
ALTER ICEBERG TABLE {sfDB}.{sfSchema}.{sfTable} REFRESH

create or replace iceberg table dataops_source.nutrislice.locations_iceberg
external_volume = 'cgna_lake_iceberg'
catalog = 'cgna_nutrislice_catalog'
catalog_table_name = 'locations_dimension';

create or replace iceberg table dataops_source.nutrislice.foods_iceberg
external_volume = 'cgna_lake_iceberg'
catalog = 'cgna_nutrislice_catalog'
catalog_table_name = 'foods_dimension';

grant select on all iceberg tables in database dataops_source to role dataops_all_db_read_role;
grant select on future iceberg tables in database dataops_source to role dataops_all_db_read_role;

create or replace iceberg table dataops_source.nutrislice.order_items_iceberg
external_volume = 'cgna_lake_iceberg'
catalog = 'cgna_nutrislice_catalog'
catalog_table_name = 'order_items';


-- CREATE HAPPY OR NOT INTEGRAGION
create or replace catalog integration cgna_happyornot_catalog
catalog_source    = GLUE
catalog_namespace = 'happyornot' -- change this to whatever the namespace is in iceberg
table_format      = ICEBERG
glue_aws_role_arn = 'arn:aws:iam::736888855894:role/cgna-snowflake_prod'
glue_catalog_id   = '736888855894'
enabled           = true;

-- create surveyresults table
create or replace iceberg table dataops_source.happyornot.survey_results
external_volume = 'cgna_lake_iceberg'
catalog = 'cgna_happyornot_catalog'
catalog_table_name = 'survey_results';


-- CG_DW_US
create or replace iceberg table dataops_source.cg_dw_us.can_pos_order_detail
external_volume = 'cgna_lake_iceberg'
catalog = 'cgna_cg_dw_us_catalog'
catalog_table_name = 'can_pos_order_detail_avec';

-- CANTEEN
create or replace catalog integration cgna_canteen_catalog
catalog_source    = GLUE
catalog_namespace = 'canteen' -- change this to whatever the namespace is in iceberg
table_format      = ICEBERG
glue_aws_role_arn = 'arn:aws:iam::736888855894:role/cgna-snowflake_prod'
glue_catalog_id   = '736888855894'
enabled           = true;

-- SNOWFLAKE_METADATA
create or replace catalog integration cgna_snowflake_metadata_catalog
catalog_source    = GLUE
catalog_namespace = 'snowflake_metadata' -- change this to whatever the namespace is in iceberg
table_format      = ICEBERG
glue_aws_role_arn = 'arn:aws:iam::736888855894:role/cgna-snowflake_prod'
glue_catalog_id   = '736888855894'
enabled           = true;

-- marketing cloud

create or replace catalog integration cgna_marketing_cloud_catalog
catalog_source    = GLUE
catalog_namespace = 'marketing_cloud' -- change this to whatever the namespace is in iceberg
table_format      = ICEBERG
glue_aws_role_arn = 'arn:aws:iam::736888855894:role/cgna-snowflake_prod'
glue_catalog_id   = '736888855894'
enabled           = true;

-- smartsheets

create or replace catalog integration cgna_smartsheets_catalog
catalog_source    = GLUE
catalog_namespace = 'smartsheets' -- change this to whatever the namespace is in iceberg
table_format      = ICEBERG
glue_aws_role_arn = 'arn:aws:iam::736888855894:role/cgna-snowflake_prod'
glue_catalog_id   = '736888855894'
enabled           = true;

-- mystaff

create or replace catalog integration cgna_mystaff_catalog
catalog_source    = GLUE
catalog_namespace = 'mystaff' -- change this to whatever the namespace is in iceberg
table_format      = ICEBERG
glue_aws_role_arn = 'arn:aws:iam::736888855894:role/cgna-snowflake_prod'
glue_catalog_id   = '736888855894'
enabled           = true;

