import pandas as pd
#from sqlalchemy import create_engine
import pyodbc
import json
import boto3
import base64
from typing import Any
from botocore.exceptions import ClientError
import boto3
from io import StringIO #python3

from pyspark import SparkContext
from pyspark.sql import SparkSession
import numpy as np
from pyspark.sql.types import StringType, TimestampType, IntegerType, DoubleType, FloatType, LongType,StructType,StructField,BooleanType,TimestampType

s3 = boto3.client('s3', aws_access_key_id='key', aws_secret_access_key='secret_key')

'''def get_connection():
    database="tRepoCrothall"
    #driver="ODBC Driver 17 for SQL Server"
    username = "svcEsfmAndCrothall"
    server = "LFTestSQL.NA.CompassGroup.Corp"
    password = "Laserfiche1"
    db_conn = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};TrustServerCertificate=yes'
    print("db_conn is:",db_conn)
    engine = pyodbc.connect(db_conn)
    print("engine is:",engine)
    df=pd.read_sql_query("select * from [dbo].[dbprop]",engine)
    df1=pd.read_sql_query("select * from [dbo].[ann]",engine)
    print("df is:",df)
    print("df is:",df1)
'''

def ingest_toc():
    db="tRepoEurest"
    schema = "dbo"
    engine = get_sql_conn(database=db)
    table = "toc"
    df = convert_tbl_df(engine=engine, database=db, schema=schema, table=table)
    print(df.describe())

def get_secret(secret_name: str, region_name: str) -> Any:

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if "SecretString" in response:
            try:
                secret = json.loads(response["SecretString"])
            except Exception as e:
                secret = response["SecretString"]
        else:
            secret = base64.b64decode(response["SecretBinary"])

        return secret
def get_sql_conn(database):
    print('get_sql_conn')
    secret_name = "dev/laserfiche"
    region_name = "us-east-1"
    secret = get_secret(secret_name, region_name)
    username = secret["username"]
    server = secret["server"]
    password = secret["password"]

    print(f"database:{database}")  # tRepoCrothall
    db_conn = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};TrustServerCertificate=yes'
    print("db_conn is:", db_conn)
    engine = pyodbc.connect(db_conn)
    return engine

def get_tbl_list(engine,database,schema):
    print('get_tbl_list')
    df = pd.read_sql_query(f"select TABLE_NAME from [INFORMATION_SCHEMA].[TABLES] where TABLE_CATALOG='{database}' and TABLE_SCHEMA='{schema}' and TABLE_TYPE='BASE TABLE' order by TABLE_NAME", engine)
    print("Tables are:", df)
    tables_list=df['TABLE_NAME'].tolist()
    return tables_list

def convert_tbl_df(engine,database,schema,table):
    print('convert_tbl_df')
    #print(f"Table Name is:{table}")
    query=f"select * from {database}.{schema}.{table}"
    print(f"Table Query Is :{query}")
    df = pd.read_sql_query(query,engine)

    print("Table Columns are:",df.columns)
    #print("Table Data is:", df)
    return df


def copy_to_s3(client, df, bucket, filepath):
    csv_buf = StringIO()
    df.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    response=client.put_object(Bucket=bucket, Body=csv_buf.getvalue(), Key=filepath)
    print(f'Copy {df.shape[0]} rows to S3 Bucket {bucket} at {filepath}, Done!')
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if status == 200:
        print(f"Successful S3 put_object response. Status - {status}")
    else:
        print(f"Unsuccessful S3 put_object response. Status - {status}")

def replace_na(df):
    for field_name, field_type in dict(df.dtypes).items():
        if field_type == "object":
            df[field_name] = df[field_name].fillna('')
        elif field_type == "int32":
            df[field_name] = df[field_name].fillna(0)
        elif field_type == "int64":
            df[field_name] = df[field_name].fillna(0)
        elif field_type == "float32":
            df[field_name] = df[field_name].fillna(0.0)
        elif field_type == "float64":
            df[field_name] = df[field_name].fillna(0.0)
        elif field_type == "bool":
            df[field_name] = df[field_name].fillna(pd.NA)
    return df;

def convert_datatype_str(df):
    for field_name, field_type in dict(df.dtypes).items():
           df[field_name] = df[field_name].astype("string")

    return df
def build_schema(df):
    fields = []
    for field_name, field_type in dict(df.dtypes).items():
        if field_type == "object":
            type_ = StringType()
        elif field_type == "int32":
            type_ = IntegerType()
        elif field_type == "int64":
            type_ = LongType()
        elif field_type == "float32":
            type_ = FloatType()
        elif field_type == "float64":
            type_ = DoubleType()
        elif field_type == "bool":
            type_ = BooleanType()
        #elif field_type == "datetime64":
        #    type_ = TimestampType()
        else:
            raise TypeError(f"Type: {field_type} is not handled!")
        fields.append(StructField(field_name, type_, True))
    schema = StructType(fields)
    return schema;


def copy_file_to_s3(spark,df,s3_bucket,filepath,table):
    full_paths = f"s3://{s3_bucket}/{filepath}"
    print("full_paths:",full_paths)
    print("df schema:",df.dtypes)
    #df['toc_modifier']=df['toc_modifier'].str.decode('utf8')
    #print("df schema1:", df.dtypes)

    try:
        if df.empty:
            upload_df = spark.createDataFrame(df, schema=build_schema(df))
            print("upload_df: Schena:", upload_df.printSchema())
            print("upload_df: Data:", upload_df.show(5))
            upload_df.coalesce(1).write.option("header", "True").option("inferSchema", "True").save(path=full_paths,
                                                                                                    format="csv",
                                                                                                    mode="overwrite")
        else:
            df = replace_na(df)
            upload_df = spark.createDataFrame(df)
            print("upload_df: Schema:", upload_df.printSchema())
            print("upload_df: Data:", upload_df.show(5))
            upload_df.coalesce(1).write.option("header", "True").option("multiLine","True").option("escape","\"").option("inferSchema", "True").save(path=full_paths,format="csv",mode="overwrite")

    except Exception as e:
        print(f'Data load failed for {table} and error is:{str(e)}')
        df = convert_datatype_str(df)
        upload_df = spark.createDataFrame(df)
        print("upload_df: Schena:", upload_df.printSchema())
        print("upload_df: Data:", upload_df.show(5))
        upload_df.coalesce(1).write.option("header", "True").option("inferSchema", "True").save(path=full_paths,
                                                                                                format="csv",
                                                                                                mode="overwrite")


if __name__ == "__main__":
    ingest_toc()
    # print("Start--------------------")
    # import argparse

    # parser = argparse.ArgumentParser()
    # parser.add_argument(
        # "--datasource-name", help="Datasource that dataset belongs to", required=True, dest="datasource"
    # )
    # parser.add_argument("--dataset-name", help="Dataset to transform on", required=True, dest="dataset")
    # parser.add_argument("--sql-database-name", help="Dataset to transform on", required=True, dest="sqldb")
    # parser.add_argument("--sql-schema-name", help="Dataset to transform on", required=True, dest="sqlschema")
    # parser.add_argument("--sql-table-name", help="Dataset to transform on", required=True, dest="sqltable")

    # args = parser.parse_args()

    # print(f"Datasource is:{args.datasource}")
    # print(f"dataset is:{args.dataset}")
    # print(f"sqldb is:{args.sqldb}")
    # print(f"sqlschema is:{args.sqlschema}")
    # print(f"sqltable is:{args.sqltable}")
    # database = args.sqldb
    # s3_bucket = "cg-data-configs-dev" #self._config["raw_data_bucket"]
    # s3_prefix = "dev_af_2.5.1/dataops-all-dags/data_engineering/aasritha_test/" #self._config["raw_data_prefix"]
    # print(f"s3_bucket:{s3_bucket}")
    # print(f"s3_prefix:{s3_prefix}")


    # engine = get_sql_conn(database)

    # _sc = SparkContext(
        # appName=f"sql server to aws s3"
    # )
    # spark = SparkSession(_sc)

    # spark.conf.set("spark.sql.session.timeZone", "UTC")

    # table = args.sqltable
    # df_to_upload = convert_tbl_df(engine, database=database,schema=args.sqlschema,table=table)
    # print(df_to_upload)
    # filepath = f"{s3_prefix}/{table}.csv"
    # print(f"filepath:{filepath}")
    # copy_file_to_s3(spark=spark, df=df_to_upload, s3_bucket=s3_bucket, filepath=filepath, table=table)

    print("End----------")
