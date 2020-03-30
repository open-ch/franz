package cmd

import (
	"context"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	metricsSocket = "/var/run/prometheus/kafka_consumerlag"
)

var (
	metricOffsetConsumer = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_consumer_lag",
			Help: "Current consumer lag for a consumer group, topic and partition",
		},
		[]string{
			"topic",
			"partition",
			"group",
		},
	)
)

var consumerLagCmd = &cobra.Command{
	Use:   "consumerlag",
	Short: "Monitor the consumer lag of a specific kafka topic.",
	Long:  `Monitor the consumer lag of a specific kafka topic.`,
	Run: func(cmd *cobra.Command, args []string) {
		consumerLag()
	},
}

var (
	maxDuration int
	minDuration int
	refresh     int
	groups      []string
	topics      []string
)

func init() {
	RootCmd.AddCommand(consumerLagCmd)

	prometheus.MustRegister(metricOffsetConsumer)
}

func consumerLag() {
	minDuration = viper.GetInt("consumerlag.minduration")
	maxDuration = viper.GetInt("consumerlag.maxduration")
	refresh = viper.GetInt("consumerlag.refresh")
	groups = viper.GetStringSlice("consumerlag.consumergroups")
	topics = viper.GetStringSlice("consumerlag.topics")

	log := getLogger()

	client, err := getKafkaClient()
	if err != nil {
		log.Fatalf("Consumer lag monitor cannot connect to Kafka: %s", err)
	}
	defer client.Close()

	scrapeConfig := newScrapeConfig(refresh, minDuration, maxDuration, topics, groups)

	ctx := context.Background()

	enforceGracefulShutdown(func(wg *sync.WaitGroup, shutdown chan struct{}) {
		startKafkaScraper(wg, shutdown, client, scrapeConfig, metricOffsetConsumer)
		startPrometheus(ctx, wg, shutdown)
	})
}

func startPrometheus(ctx context.Context, wg *sync.WaitGroup, shutdown chan struct{}) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	log := getLogger()
	srv := &http.Server{
		Addr:    "/metrics",
		Handler: promhttp.Handler(),
	}

	wg.Add(1)
	defer wg.Done()

	listener, errListen := net.Listen("unix", metricsSocket)
	if errListen != nil {
		log.Fatalf("Failed to initialize metrics socket: %s", errListen.Error())
	}
	go func() {
		httpErr := srv.Serve(listener)
		if httpErr != nil {
			log.Printf("Prometheus had an Error and shut down: %s", httpErr.Error())
		}
	}()

	<-shutdown
	log.Println("Shutting down metrics server")
	err := srv.Shutdown(ctx)
	if err != nil {
		log.Errorf("cannot shutdown server: %v", err)
	}

	err = listener.Close()
	if err != nil {
		log.Errorf("cannot close listener: %v", err)
	}
	os.Remove(metricsSocket)
}

func enforceGracefulShutdown(f func(wg *sync.WaitGroup, shutdown chan struct{})) {
	log := getLogger()
	wg := &sync.WaitGroup{}
	shutdown := make(chan struct{})
	signals := make(chan os.Signal)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	go func() {
		<-signals
		close(shutdown)
	}()

	f(wg, shutdown)

	<-shutdown
	log.Println("Initiating shutdown of consumer lag monitoring...")
	wg.Wait()
}
