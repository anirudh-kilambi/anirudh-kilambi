--  general share creation
CREATE SHARE CD_COMPASSONE_DBT_DEV_OUTBOUND
COMMENT='Outbound share of dbt_dev data to COMPASS ONE: lub72300.us-east-1.snowflakecomputing.com';

create database if not exists compassone_dbt_dev_outbound;

grant usage on database compassone_dbt_dev_outbound to share CD_COMPASSONE_DBT_DEV_OUTBOUND;

GRANT REFERENCE_USAGE ON DATABASE dbt_dev TO SHARE CD_COMPASSONE_DBT_DEV_OUTBOUND;

ALTER SHARE CD_COMPASSONE_DBT_DEV_OUTBOUND ADD ACCOUNT = "CGNA.COMPASSONE_DATA";

-- CREATE SCHEMA DIGITAL_LEARNING
USE database compassone_dbt_dev_outbound;

create schema if not exists digital_learning;

grant usage on schema digital_learning to share CD_COMPASSONE_DBT_DEV_OUTBOUND;

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


