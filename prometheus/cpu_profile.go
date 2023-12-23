package prometheus

import (
	"encoding/json"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"hermes/parser"
	"path/filepath"
	"strconv"
)

type CpuExporter struct {
	viewDir         string
	cpuPercent      *prometheus.GaugeVec
	cpuTotalPercent prometheus.Gauge
}

func NewCpuExporter(namespace string, viewDir string) *CpuExporter {
	return &CpuExporter{
		viewDir: viewDir,
		cpuPercent: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "percent",
			Help:      "the cpu percentage consumed",
		},
			[]string{"comm"},
		),
		cpuTotalPercent: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "percent",
			Help:      "the cpu percentage consumed",
		}),
	}
}

func (c *CpuExporter) Describe(ch chan<- *prometheus.Desc) {
	c.cpuPercent.Describe(ch)

}

func (c *CpuExporter) Collect(ch chan<- prometheus.Metric) {
	c.collectParsed(ch)
}

// collect data from post-parsed hermes data files
func (c *CpuExporter) collectParsed(ch chan<- prometheus.Metric) {
	// get overview
	var overviewRecs []parser.CpuInfoRecord
	overviewBytes, err := GetBytesFromFile(filepath.Join(c.viewDir, parser.CpuProfileJob, "overview"))
	if err != nil {
		logrus.Errorf("collectParsed: error getting bytes from file [%s]", err)
		return
	}

	if err := json.Unmarshal(overviewBytes, &overviewRecs); err != nil {
		logrus.Errorf("collectParsed: error unmarshalling overview data [%s]", err)
		return
	}

	// get cpu percent for latest
	var latest int64
	var cpuPercent uint64
	var triggered bool
	for i, rec := range overviewRecs {
		if rec.Timestamp > latest || i == 0 {
			latest = rec.Timestamp
			cpuPercent = rec.Val
			triggered = rec.Triggered
		}
	}

	c.cpuTotalPercent.Set(float64(cpuPercent))
	c.cpuTotalPercent.Collect(ch)

	// if triggered, also collect per process percent
	if triggered {

		bytes, err := GetBytesFromFile(filepath.Join(c.viewDir, parser.CpuProfileJob, strconv.Itoa(int(latest)), parser.ParsedPostfix[parser.CpuProfileJob]))
		if err != nil {
			logrus.Errorf("collectParsed: error getting latest data [%s]", err)
		}
		jsonData := string(bytes)

		total := gjson.Get(jsonData, "value").Float()

		// array of procs in cpu profile
		procs := gjson.Get(jsonData, "children").Array()
		for _, rec := range procs {
			name := rec.Map()["name"].String()
			if len(name) > 0 {
				value := rec.Map()["value"].Float()
				labels := prometheus.Labels{"comm": name}
				c.cpuPercent.With(labels).Set(value / total * float64(cpuPercent))
			}
		}
		c.cpuPercent.Collect(ch)
	}
	return
}
