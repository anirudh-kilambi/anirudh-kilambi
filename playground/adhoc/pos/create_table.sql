spark.sql("""
    select * into iceberg_catalog.cg_dw_us.can_pos_order_detail from iceberg_catalog.cg_dw_us.can_pos_order_detail_avec
    UNION ALL
    select * into iceberg_catalog.cg_dw_us.can_pos_order_detail from iceberg_catalog.cg_dw_us.can_pos_order_detail_micros"""
)

spark.sql("""
    create table can_pos_order_detail as (
    select * from can_pos_order_detail_avec
    UNION ALL
    select * from can_pos_order_detail_micros
    UNION ALL
    select * from can_pos_order_detail_avec_pilot
    UNION ALL
    select * from can_pos_order_detail_nextep
    UNION ALL
    select * from can_pos_order_detail_ocs
    UNION ALL
    select * from can_pos_order_detail_ocs_pilot
    UNION ALL
    select * from can_pos_order_detail_volante_pilot
)"""
)
spark.sql("""
    create table can_pos_order_detail as (
    select * from can_pos_order_detail_avec
)"""
)
