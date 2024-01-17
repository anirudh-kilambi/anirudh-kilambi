desc warehouse warehouse_name;

create warehouse if not exists warehouse_name
warehouse_size=xsmall
MAX_CLUSTER_COUNT = 2
MIN_CLUSTER_COUNT = 1
AUTO_RESUME = TRUE
ENABLE_QUERY_ACCELERATION = TRUE
QUERY_ACCELERATION_MAX_SCALE_FACTOR = 5
AUTO_SUSPEND = 60;

grant usage on warehouse warehouse_name to role role_name;

grant operate on warehouse warehouse_name to role role_name;
grant monitor on warehouse warehouse_name to role role_name;
grant modify on warehouse warehouse_name to role role_name;

alter warehouse warehouse_name ABORT ALL QUERIES;

