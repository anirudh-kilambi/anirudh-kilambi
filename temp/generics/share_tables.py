import json
import boto3
from pyspark import SparkContext
from pyspark.sql import SparkSession


def share_tables(source_db:str, source_schema:str, share_db:str, share_schema:str, share_name:str):
    """
    Generate the create and grant queries for an entire schema
    """
    sc = SparkContext(
        appName=f"Add shares {source_db}.{source_schema} to {share_db}.{share_schema}"
    )
    spark = SparkSession(sc)
    secrets_manager = boto3.client("secretsmanager", region_name="us-east-1")

    secret = json.loads(
        secrets_manager.get_secret_value(
            SecretId="DATAOPS_SNOWFLAKE_US_EAST_1_BATCH_USER"
        )["SecretString"]
    )
    sfOptions = {
        "sfURL": f"{secret['account']}.privatelink.snowflakecomputing.com",
        "sfUser": secret["user"],
        "sfPassword": secret["password"],
        "sfDatabase": source_db,
        "sfSchema": source_schema,
        "sfWarehouse": "DATAOPS_BATCH_WH",
    }

    SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"


    tables_query = f"""
    SELECT * from {source_db}.information_schema.tables
    where TABLE_SCHEMA = '{source_schema}'
    """

    tables_df = (
        spark.read.format(SNOWFLAKE_SOURCE_NAME)
        .options(**sfOptions)
        .option("query", tables_query)
        .load()
    )
    list_of_tables = tables_df.collect()
    
    tables = [i["TABLE_NAME"] for i in list_of_tables]

    for table in tables:
        create_query = f"""
        create or replace secure view {share_db}.{share_schema}.{table}
        as (select * from {source_db}.{source_schema}.{table})
        """
        print(create_query)

        grant_query = f"""
        grant select on view {share_db}.{share_schema}.{table} to share {share_name}
        """
        print(grant_query)

        sc._jvm.net.snowflake.spark.snowflake.Utils.runQuery(sfOptions, create_query)
        sc._jvm.net.snowflake.spark.snowflake.Utils.runQuery(sfOptions, grant_query)

    usage_query = f"""
    grant usage on schema {share_db}.{share_schema} to share {share_name}
    """

    # print(usage_query)

    reference_usage_query = f"""
    grant reference_usage on database {source_db} to share {share_name}
    """
    # print(reference_usage_query)

    # sc._jvm.net.snowflake.spark.snowflake.Utils.runQuery(sfOptions, usage_query)
    # sc._jvm.net.snowflake.spark.snowflake.Utils.runQuery(sfOptions, reference_usage_query)

def get_tables(source_db:str, source_schema:str):
    """
    Generate the create and grant queries for an entire schema
    """
    sc = SparkContext(
        appName=f"Add shares {source_db}.{source_schema}"
    )
    spark = SparkSession(sc)
    secrets_manager = boto3.client("secretsmanager", region_name="us-east-1")

    secret = json.loads(
        secrets_manager.get_secret_value(
            SecretId="DATAOPS_SNOWFLAKE_US_EAST_1_BATCH_USER"
        )["SecretString"]
    )
    sfOptions = {
        "sfURL": f"{secret['account']}.privatelink.snowflakecomputing.com",
        "sfUser": secret["user"],
        "sfPassword": secret["password"],
        "sfDatabase": source_db,
        "sfSchema": source_schema,
        "sfWarehouse": "DATAOPS_BATCH_WH",
    }

    SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"


    tables_query = f"""
    SELECT * from {source_db}.information_schema.tables
    where TABLE_SCHEMA = '{source_schema}'
    """

    tables_df = (
        spark.read.format(SNOWFLAKE_SOURCE_NAME)
        .options(**sfOptions)
        .option("query", tables_query)
        .load()
    )
    list_of_tables = tables_df.collect()

    tables = [i["TABLE_NAME"] for i in list_of_tables]

    for table in tables:
        print(f"{source_db}.{source_schema}.{table}" +  " : {}")

if __name__ == "__main__":

    source_db = "dataops_source"
    source_schema = "LASERFICHE_APDATA"
    share_db  = "compassgroup_source_outbound"
    share_schema = "LASERFICHE_APDATA"
    share_name = "cd_compassgroup_source_outbound"

    get_tables(
        source_db=source_db,
        source_schema=source_schema
    )
