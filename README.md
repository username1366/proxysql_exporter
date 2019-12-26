# proxysql_exporter
Prometheus proxySQL exporter

```sh
DEBUG=1 SOCKET=:9032 MYSQL_DSN="admin:password@tcp(localhost:1033)/stats" go run main.go
```

# list of supported metrics
```
proxysql_conn_error
proxysql_conn_ok
proxysql_latency_ns
proxysql_queries
proxysql_recv_bytes
proxysql_sent_bytes
proxysql_up
```
