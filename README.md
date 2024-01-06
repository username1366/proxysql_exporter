# proxysql_exporter
Prometheus proxySQL exporter

```sh
DEBUG=1 SOCKET=:9032 MYSQL_DSN="admin:password@tcp(localhost:1033)/stats" go run main.go

```env
# templates accept LIKE style sql syntax value
# example: 'begin tran%', 'select [0-9]'
INCLUDE_QUERY_PATTERN - query templates to include in metrics
EXCLUDE_QUERY_PATTERN - query templates to exclude from metrics

```
entryType: true - include | flase - exclude query pattern

# list of supported metrics
```
proxysql_conn_error
proxysql_conn_ok
proxysql_conn_used
proxysql_conn_free
proxysql_latency_ns
proxysql_queries
proxysql_recv_bytes
proxysql_sent_bytes
proxysql_up
proxysql_query_count_total
proxysql_query_min_time
proxysql_query_max_time
```
