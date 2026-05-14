package main

import (
	"log"
	"net/http"
	"sort"

	sio "github.com/faceair/socket.io-cluster"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type sioCollector struct{ server *sio.Server }

func (c sioCollector) Describe(ch chan<- *prometheus.Desc) {}

func (c sioCollector) Collect(ch chan<- prometheus.Metric) {
	for _, sample := range c.server.Metrics().Samples {
		labelNames := make([]string, 0, len(sample.Labels))
		for name := range sample.Labels {
			labelNames = append(labelNames, name)
		}
		sort.Strings(labelNames)
		labelValues := make([]string, 0, len(labelNames))
		for _, name := range labelNames {
			labelValues = append(labelValues, sample.Labels[name])
		}
		valueType := prometheus.GaugeValue
		if sample.Kind == sio.MetricCounter {
			valueType = prometheus.CounterValue
		}
		desc := prometheus.NewDesc(sample.Name, sample.Help, labelNames, nil)
		ch <- prometheus.MustNewConstMetric(desc, valueType, sample.Value, labelValues...)
	}
}

func main() {
	server, err := sio.NewServer(&sio.ServerConfig{
		AcceptAnyNamespace: true,
		Port:               "3000",
		OnError:            func(err error) { log.Println(err) },
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = server.Close() }()

	server.OnConnection(func(socket sio.ServerSocket) {
		socket.OnEvent("ping", func(ack func(string)) { ack("pong") })
	})

	registry := prometheus.NewRegistry()
	registry.MustRegister(sioCollector{server: server})

	http.Handle("/socket.io/", server)
	http.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	log.Fatal(http.ListenAndServe(":3000", nil))
}
