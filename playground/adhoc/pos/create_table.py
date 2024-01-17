from pyspark.sql import SparkSession

spark = SparkSession.builder.appName(f"can pos order detail load").getOrCreate()
print(f"Purging can pos order detail ")
spark.sql("drop table if exists iceberg_catalog.cg_dw_us.can_pos_order_detail")

print("Creating table with avec partition")
spark.sql("""
    create table can_pos_order_detail as (
    select * from can_pos_order_detail_avec
)""")

print("Inserting MICROS partition into table.")
spark.sql("""
    insert into iceberg_catalog.cg_dw_us.can_pos_order_detail (
    select * from can_pos_order_detail_micros
)""")

print("Inserting avec_pilot partition into table.")
spark.sql("""
    insert into iceberg_catalog.cg_dw_us.can_pos_order_detail (
    select * from can_pos_order_detail_avec_pilot
)""")

print("Inserting nextep partition into table.")
spark.sql("""
    insert into iceberg_catalog.cg_dw_us.can_pos_order_detail (
    select * from can_pos_order_detail_nextep
)""")

print("Inserting ocs partition into table.")
spark.sql("""
    insert into iceberg_catalog.cg_dw_us.can_pos_order_detail (
    select * from can_pos_order_detail_ocs
)""")

print("Inserting ocs_pilot partition into table.")
spark.sql("""
    insert into iceberg_catalog.cg_dw_us.can_pos_order_detail (
    select * from can_pos_order_detail_ocs_pilot
)""")

print("Inserting volante_pilot partition into table.")
spark.sql("""
    insert into iceberg_catalog.cg_dw_us.can_pos_order_detail (
    select * from can_pos_order_detail_volante_pilot
)""")

