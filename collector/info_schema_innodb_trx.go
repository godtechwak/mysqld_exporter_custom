// Copyright 2018 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Scrape `information_schema.processlist`.

package collector

import (
	"context"
	"fmt"

	"github.com/alecthomas/kingpin/v2"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
)

// 롱 트랜잭션 체크 쿼리
const infoSchemaInnodbTrxQuery = `		
		  SELECT (unix_timestamp(now()) - unix_timestamp(trx_started)) AS trx_seconds
			FROM information_schema.innodb_trx tx
			     JOIN information_schema.processlist p ON p.id=tx.trx_mysql_thread_id
		   WHERE tx.trx_state IN ('RUNNING','LOCK WAIT','ROLLING BACK','COMMITTING')
  			 AND (unix_timestamp(now()) - unix_timestamp(tx.trx_started)) > %d
		   ORDER BY trx_seconds DESC
		   LIMIT 1
	`

// Tunable flags.
/** infoSchemaInnodbTrxQuery 쿼리 파라미터에 사용할 플래그 **/
var (
	trxMinTime = kingpin.Flag(
		"collect.info_schema.innodb_trx.min_seconds",
		"Minimum time a thread must be in each state to be counted",
	).Default("0").Int()
)

// Metric descriptors.
// 생성할 메트릭명
// ex) mysql_info_schema_trx_seconds
// 3번째 파라미터는 메트릭에서 사용될 인자
var (
	trxTimeDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, informationSchema, "trx_seconds"),
		"Execution threshold time of long transactions",
		nil, nil)
)

// ScrapeProcesslist collects from `information_schema.processlist`.
type ScrapeInnodbTrx struct{}

// Name of the Scraper. Should be unique.
func (ScrapeInnodbTrx) Name() string {
	return informationSchema + ".innodb_trx"
}

// Help describes the role of the Scraper.
func (ScrapeInnodbTrx) Help() string {
	return "Collect current transaction counts from the information_schema.innodb_trx"
}

// Version of MySQL from which scraper is available.
func (ScrapeInnodbTrx) Version() float64 {
	return 5.1
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (ScrapeInnodbTrx) Scrape(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger log.Logger) error {
	trxQuery := fmt.Sprintf(
		infoSchemaInnodbTrxQuery,
		*trxMinTime,
	)
	db := instance.getDB()
	innodbTrxRows, err := db.QueryContext(ctx, trxQuery)
	if err != nil {
		return err
	}
	defer innodbTrxRows.Close()

	var (
		trx_seconds uint32
	)

	for innodbTrxRows.Next() {
		err = innodbTrxRows.Scan(&trx_seconds)

		if err != nil {
			return err
		}

		// 첫 번째 인자 값은 Metric descriptors
		ch <- prometheus.MustNewConstMetric(trxTimeDesc, prometheus.GaugeValue, float64(trx_seconds))
	}

	return nil
}

var _ Scraper = ScrapeInnodbTrx{}
