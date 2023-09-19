package main

import (
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"time"

	log "github.com/sirupsen/logrus"

	_ "github.com/go-sql-driver/mysql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	up              *prometheus.GaugeVec
	connectionError *prometheus.GaugeVec
	connectionOK    *prometheus.GaugeVec
	connectionUsed  *prometheus.GaugeVec
	connectionFree  *prometheus.GaugeVec
	queries         *prometheus.GaugeVec
	sentBytes       *prometheus.GaugeVec
	recvBytes       *prometheus.GaugeVec
	latencyNs       *prometheus.GaugeVec
	countStar       *prometheus.GaugeVec
	minTime         *prometheus.GaugeVec
	maxTime         *prometheus.GaugeVec
)

// Initialize gauges
func init() {
	switch os.Getenv("DEBUG") {
	case "1", "true", "enabled":
		log.SetLevel(log.DebugLevel)
	}

	up = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "proxysql_up",
		}, []string{})
	prometheus.MustRegister(up)

	connectionError = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "proxysql_conn_error",
			Help: "how many connections were not established successfully",
		}, []string{"hostgroup", "srv_host", "srv_port", "status"})
	prometheus.MustRegister(connectionError)

	connectionOK = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "proxysql_conn_ok",
			Help: "how many connections were established successfully",
		}, []string{"hostgroup", "srv_host", "srv_port", "status"})
	prometheus.MustRegister(connectionOK)

	connectionUsed = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "proxysql_conn_used",
			Help: "how many connections are currently used by ProxySQL for sending queries to the backend server",
		}, []string{"hostgroup", "srv_host", "srv_port", "status"})
	prometheus.MustRegister(connectionUsed)

	connectionFree = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "proxysql_conn_free",
			Help: "how many connections are currently free. They are kept open in order to minimize the time cost of sending a query to the backend server",
		}, []string{"hostgroup", "srv_host", "srv_port", "status"})
	prometheus.MustRegister(connectionFree)

	queries = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "proxysql_queries",
			Help: "the number of queries routed towards this particular backend server",
		}, []string{"hostgroup", "srv_host", "srv_port", "status"})
	prometheus.MustRegister(queries)

	sentBytes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "proxysql_sent_bytes",
			Help: "the amount of data sent to the backend. This does not include metadata (packets headers)",
		}, []string{"hostgroup", "srv_host", "srv_port", "status"})
	prometheus.MustRegister(sentBytes)

	recvBytes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "proxysql_recv_bytes",
			Help: "the amount of data received from the backend. This does not include metadata (packets headers, OK/ERR packets, fields description, etc)",
		}, []string{"hostgroup", "srv_host", "srv_port", "status"})
	prometheus.MustRegister(recvBytes)

	latencyNs = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "proxysql_latency_ns",
			Help: "the current ping time in microseconds, as reported from Monitor",
		}, []string{"hostgroup", "srv_host", "srv_port", "status"})
	prometheus.MustRegister(latencyNs)

	countStar = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "proxysql_query_count_total",
			Help: "the total number of times the query has been executed (with different values for the parameters)",
		}, []string{"hostgroup", "schemaname", "digest", "digest_text"})
	prometheus.MustRegister(countStar)

	minTime = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "proxysql_query_min_time",
			Help: "the total time in microseconds spent executing queries of this type",
		}, []string{"hostgroup", "schemaname", "digest", "digest_text"})
	prometheus.MustRegister(minTime)

	maxTime = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "proxysql_query_max_time",
			Help: "the total time in microseconds spent executing queries of this type",
		}, []string{"hostgroup", "schemaname", "digest", "digest_text"})
	prometheus.MustRegister(maxTime)
}

func main() {
	mysqlDSN := os.Getenv("MYSQL_DSN")
	if len(mysqlDSN) < 1 {
		log.Errorf("MYSQL_DNS isn't set")
		os.Exit(1)
	}

	socket := os.Getenv("SOCKET")
	if len(socket) < 1 {
		log.Errorf("SOCKET isn't set")
		os.Exit(1)
	}

	go GetStats(mysqlDSN)

	log.Printf("Listen on %v", socket)
	http.Handle("/metrics", promhttp.Handler())
	log.Println(http.ListenAndServe(socket, nil))
}

var globalDB *sql.DB

func NewConnect(mysqlDSN string) (*sql.DB, error) {
	var err error
	if globalDB == nil {
		globalDB, err = sql.Open("mysql", mysqlDSN)
		if err != nil {
			return nil, err
		}
		return globalDB, nil
	}
	log.Debugf("Reuse conncection")
	return globalDB, nil
}

// Get statistics from memory DB proxysql
func GetStats(mysqlDSN string) {
	var err error
	var db *sql.DB
	// var result sql.Result
	for {
		db, err = NewConnect(mysqlDSN)
		if err != nil {
			log.Errorf("DB connection error. %v. Try in 9 seconds", err)
			up.With(prometheus.Labels{}).Set(float64(0))
			time.Sleep(time.Second * 9)
			continue
		}

		// result, err = db.Exec("use stats")
		// if err != nil {
		// 	log.Errorf("Use database error. %v. Try in 5 seconds", err)
		// 	up.With(prometheus.Labels{}).Set(float64(0))
		// 	time.Sleep(time.Second * 5)
		// 	continue
		// }
		// log.Debugf("result: %v", result)

		err = GetStatConnectionPool(db)
		if err != nil {
			log.Errorf("Query get connection_pool execute error: %v. Try in 9 seconds", err)
			up.With(prometheus.Labels{}).Set(float64(0))
			time.Sleep(time.Second * 9)
			continue
		}
		err = GetStatQueryDigest(db)
		if err != nil {
			log.Errorf("Query get query_digest execute error: %v. Try in 9 seconds", err)
			up.With(prometheus.Labels{}).Set(float64(0))
			time.Sleep(time.Second * 9)
			continue
		}

		up.With(prometheus.Labels{}).Set(float64(1))

		time.Sleep(time.Second * 9)
	}
	defer db.Close()
}

// retrieves stats from stats.stats_mysql_connection_pool table
func GetStatConnectionPool(db *sql.DB) error {
	var err error
	var rows *sql.Rows

	rows, err = db.Query("SELECT * FROM stats.stats_mysql_connection_pool")
	if err != nil {
		// log.Errorf("Query execute error: %v.", err)
		return err
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
			return err
		}

		log.Debugln(hostgroup, srvHost, srvPort, status, ConnUsed, ConnFree, ConnOK, ConnERR, MaxConnUsed, Queries, QueriesGTIDSync, BytesDataSent, BytesDataRecv, LatencyUs)

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

		connectionUsed.With(prometheus.Labels{
			"hostgroup": fmt.Sprintf("%v", hostgroup),
			"srv_host":  srvHost,
			"srv_port":  fmt.Sprintf("%v", srvPort),
			"status":    status,
		}).Set(float64(ConnUsed))

		connectionFree.With(prometheus.Labels{
			"hostgroup": fmt.Sprintf("%v", hostgroup),
			"srv_host":  srvHost,
			"srv_port":  fmt.Sprintf("%v", srvPort),
			"status":    status,
		}).Set(float64(ConnFree))

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
	}
	return nil
}

// retrieves stats from stats.stats_mysql_query_digest table
func GetStatQueryDigest(db *sql.DB) error {
	var err error
	var rows *sql.Rows

	rows, err = db.Query("select ifnull(hg.comment, cast(qd.hostgroup as varchar)) as hostgroup, qd.schemaname, qd.digest, qd.digest_text, sum(qd.count_star) as count_star, min(qd.min_time) as min_time, max(qd.max_time) as max_time from stats_mysql_query_digest qd left join runtime_mysql_replication_hostgroups hg on qd.hostgroup = hg.writer_hostgroup or qd.hostgroup = hg.reader_hostgroup group by ifnull(hg.comment, cast(qd.hostgroup as varchar)), qd.schemaname, qd.digest, qd.digest_text order by qd.count_star desc limit 10")
	if err != nil {
		return err
	}

	for rows.Next() {
		var (
			hostgroup   string
			schemaname  string
			digest      string
			digest_text string
			count_star  int
			min_time    int
			max_time    int
		)
		err = rows.Scan(&hostgroup, &schemaname, &digest, &digest_text, &count_star, &min_time, &max_time)
		if err != nil {
			return err
		}

		log.Debugln(hostgroup, schemaname, digest, digest_text, count_star, min_time, max_time)

		countStar.With(prometheus.Labels{
			"hostgroup":   hostgroup,
			"schemaname":  schemaname,
			"digest":      digest,
			"digest_text": digest_text,
		}).Set(float64(count_star))

		minTime.With(prometheus.Labels{
			"hostgroup":   hostgroup,
			"schemaname":  schemaname,
			"digest":      digest,
			"digest_text": digest_text,
		}).Set(float64(min_time))

		maxTime.With(prometheus.Labels{
			"hostgroup":   hostgroup,
			"schemaname":  schemaname,
			"digest":      digest,
			"digest_text": digest_text,
		}).Set(float64(max_time))
	}
	return nil
}
