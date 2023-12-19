package prometheus

import (
        "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"io/ioutil"
	"hermes/parser"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"path/filepath"
)

const (
	IO = parser.IoLatencyJob // "io_latency"
	CPU = parser.CpuProfileJob // "cpu_profile"
	MEM = parser.MemleakProfileJob // "memleak_profile"
	PREFIX = "hermes"
)

// Names of post-parse data files
var dataFilenames map[string]string = map[string]string{
	IO: "blk_ios.json",
	CPU: "overall_cpu.stack.json",
	MEM: "slab.stack.json",
}

func HermesPrometheusHandler(reg prometheus.Gatherer) gin.HandlerFunc {
	h := promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
	return func(c *gin.Context) {
        	h.ServeHTTP(c.Writer, c.Request)
    	}
}

// Overarching exporter, with 'sub exporters'
type HermesExporter struct{
	Io *IoLatExporter
}

func NewHermesExporter(sourceDir string) *HermesExporter {
	logrus.Info("creating new hermes exporter")
	return &HermesExporter{
		Io: NewIoLatExporter(sourceDir),
	}	
}

func (h *HermesExporter) Collect(ch chan<- prometheus.Metric) {
	logrus.Info("hermes collecting")
	h.Io.Collect(ch)
}

func (h *HermesExporter) Describe(ch chan<- *prometheus.Desc) {
	logrus.Info("hermes describing")
	h.Io.Describe(ch)
}


// Return latest data as bytes
func GatherDataAsBytes(path string, kind string) ([]byte, error){
	// get latest file
	sourceDir := filepath.Join(path, kind)
	latestDir, err := GetLatestDataDir(sourceDir)
	if err != nil {
		return nil, err
	}

	// gather per dev stats
	latestFile := filepath.Join(latestDir, dataFilenames[kind])
	return ioutil.ReadFile(latestFile)
}

// Return the most up to date timestamp (dir) containing parsed metrics
func GetLatestDataDir(path string) (ret string, err error) {
	dirItems, err := ioutil.ReadDir(path)
	if err != nil {
		return
	}
	if len(dirItems) > 1 {
		latest := dirItems[0]
		for _, item := range dirItems {
			// check is dir TODO: also validate dirname is timestamp string
			if item.IsDir() {
				if latest.Name() <= item.Name() &&
					latest.Name() != "overview" {
					latest = item
				}
			}
		}
		ret = filepath.Join(path, latest.Name())
	}
	return
}

