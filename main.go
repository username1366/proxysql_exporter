package main

import (
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	connectionError *prometheus.GaugeVec
	connectionOK    *prometheus.GaugeVec
	queries         *prometheus.GaugeVec
	sentBytes       *prometheus.GaugeVec
	recvBytes       *prometheus.GaugeVec
	latencyNs       *prometheus.GaugeVec
	up              *prometheus.GaugeVec
)

// Initialize gauges
func init() {
	connectionError = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "proxysql_conn_error",
		}, []string{"hostgroup", "srv_host", "srv_port", "status"})
	prometheus.MustRegister(connectionError)

	connectionOK = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "proxysql_conn_ok",
		}, []string{"hostgroup", "srv_host", "srv_port", "status"})
	prometheus.MustRegister(connectionOK)

	queries = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "proxysql_queries",
		}, []string{"hostgroup", "srv_host", "srv_port", "status"})
	prometheus.MustRegister(queries)

	sentBytes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "proxysql_sent_bytes",
		}, []string{"hostgroup", "srv_host", "srv_port", "status"})
	prometheus.MustRegister(sentBytes)

	recvBytes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "proxysql_recv_bytes",
		}, []string{"hostgroup", "srv_host", "srv_port", "status"})
	prometheus.MustRegister(recvBytes)

	latencyNs = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "proxysql_latency_ns",
		}, []string{"hostgroup", "srv_host", "srv_port", "status"})
	prometheus.MustRegister(latencyNs)

	up = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "proxysql_up",
		}, []string{})
	prometheus.MustRegister(up)
}

func main() {
	mysqlDSN := os.Getenv("MYSQL_DSN")
	if len(mysqlDSN) < 1 {
		fmt.Printf("MYSQL_DNS isn't set")
		os.Exit(1)
	}

	socket := os.Getenv("SOCKET")
	if len(socket) < 1 {
		fmt.Printf("SOCKET isn't set")
		os.Exit(1)
	}

	go GetStats(mysqlDSN)

	fmt.Printf("Listen on %v\n", socket)
	http.Handle("/metrics", promhttp.Handler())
	fmt.Println(http.ListenAndServe(socket, nil))
}

// GetStats retrieves stats from stats.stats_mysql_connection_pool table
func GetStats(mysqlDSN string) {
	for {
		db, err := sql.Open("mysql", mysqlDSN)
		if err != nil {
			fmt.Printf("DB connection error. Try in 5 seconds\n")
			up.With(prometheus.Labels{}).Set(float64(0))
			time.Sleep(time.Second * 5)
			continue
		}

		rows, err := db.Query("SELECT * FROM stats.stats_mysql_connection_pool")
		if err != nil {
			fmt.Printf("Query execute error. Try in 5 seconds\n")
			up.With(prometheus.Labels{}).Set(float64(0))
			time.Sleep(time.Second * 5)
			continue
		}

		for rows.Next() {
			var (
				hostgroup       int
				srvHost         string
				srvPort         int
				status          string
				ConnUsed        int
				ConnFree        int
				ConnOK          int
				ConnERR         int
				MaxConnUsed     int
				Queries         int
				QueriesGTIDSync int
				BytesDataSent   int
				BytesDataRecv   int
				LatencyUs       int
			)
			err = rows.Scan(&hostgroup, &srvHost, &srvPort, &status, &ConnUsed, &ConnFree, &ConnOK, &ConnERR, &MaxConnUsed, &Queries, &QueriesGTIDSync, &BytesDataSent, &BytesDataRecv, &LatencyUs)
			if err != nil {
				fmt.Printf("Rows scan error. Try new query in 5 seconds\n")
				up.With(prometheus.Labels{}).Set(float64(0))
				time.Sleep(time.Second * 5)
				break
			}

			if len(os.Getenv("DEBUG")) > 0 {
				fmt.Println(hostgroup, srvHost, srvPort, status, ConnUsed, ConnFree, ConnOK, ConnERR, MaxConnUsed, Queries, QueriesGTIDSync, BytesDataSent, BytesDataRecv, LatencyUs)
			}

			connectionError.With(prometheus.Labels{
				"hostgroup": fmt.Sprintf("%v", hostgroup),
				"srv_host":  srvHost,
				"srv_port":  fmt.Sprintf("%v", srvPort),
				"status":    status,
			}).Set(float64(ConnERR))

			connectionOK.With(prometheus.Labels{
				"hostgroup": fmt.Sprintf("%v", hostgroup),
				"srv_host":  srvHost,
				"srv_port":  fmt.Sprintf("%v", srvPort),
				"status":    status,
			}).Set(float64(ConnOK))

			queries.With(prometheus.Labels{
				"hostgroup": fmt.Sprintf("%v", hostgroup),
				"srv_host":  srvHost,
				"srv_port":  fmt.Sprintf("%v", srvPort),
				"status":    status,
			}).Set(float64(Queries))

			sentBytes.With(prometheus.Labels{
				"hostgroup": fmt.Sprintf("%v", hostgroup),
				"srv_host":  srvHost,
				"srv_port":  fmt.Sprintf("%v", srvPort),
				"status":    status,
			}).Set(float64(BytesDataSent))

			recvBytes.With(prometheus.Labels{
				"hostgroup": fmt.Sprintf("%v", hostgroup),
				"srv_host":  srvHost,
				"srv_port":  fmt.Sprintf("%v", srvPort),
				"status":    status,
			}).Set(float64(BytesDataRecv))

			latencyNs.With(prometheus.Labels{
				"hostgroup": fmt.Sprintf("%v", hostgroup),
				"srv_host":  srvHost,
				"srv_port":  fmt.Sprintf("%v", srvPort),
				"status":    status,
			}).Set(float64(LatencyUs))

			up.With(prometheus.Labels{}).Set(float64(1))
		}
		db.Close()
		time.Sleep(time.Second * 5)
	}
}
