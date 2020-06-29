package cmd

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	envPrefix = "FRANZ"
)

var (
	cfgFile       string
	verbose       bool
	formatAsTable bool
	formatAsYAML  bool
)

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "franz",
	Short: "Your Swiss-Army Knife tool for interacting with Kafka.",
	Long: `Your Swiss-Army Knife tool for interacting with Kafka.

franz provides you various helper tools to work with and debug Kafka
such as consuming or producing messages and managing topics or ACLs.`,
}

// Execute adds all child commands to the root command sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)
	var (
		keyFile    string
		certFile   string
		caFile     string
		brokerList []string
		registry   string
	)

	pf := RootCmd.PersistentFlags()

	pf.StringArrayVarP(&brokerList, "brokers", "", []string{}, "Display messages for given brokers")
	pf.StringVar(&registry, "registry", "", "Registry endpoint")
	pf.StringVar(&cfgFile, "config", "", "Path of config file")
	pf.StringVar(&keyFile, "tls.key", "", "X509 key file in PEM encoding")
	pf.StringVar(&certFile, "tls.cert", "", "X509 cert file in PEM encoding")
	pf.StringVar(&caFile, "tls.caCert", "", "X509 Root CA file in PEM encoding")
	pf.BoolVarP(&verbose, "verbose", "v", false, "Verbose output")
	pf.BoolVarP(&formatAsTable, "table", "t", false, "Format output as table (if possible)")
	pf.BoolVarP(&formatAsYAML, "yaml", "y", false, "Format output as YAML (if possible)")

	_ = viper.BindPFlag("brokers", pf.Lookup("brokers"))
	_ = viper.BindPFlag("registry", pf.Lookup("registry"))
	_ = viper.BindPFlag("tls.key", pf.Lookup("tls.key"))
	_ = viper.BindPFlag("tls.cert", pf.Lookup("tls.cert"))
	_ = viper.BindPFlag("tls.caCert", pf.Lookup("tls.caCert"))
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	// let's attempt to set a sensible default location for the config,
	// if this fails, we rely on the config file passed explicitly
	if home, err := os.UserHomeDir(); err == nil {
		viper.SetConfigName("config")
		viper.AddConfigPath(home + "/.config/franz")
		viper.AddConfigPath("/opt/franz/etc")
	}

	// if the config file is passed explicitly, use this instead of the default
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	}
	viper.SetEnvPrefix(envPrefix)
	viper.AutomaticEnv()

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err != nil {
		fmt.Println("Can't read config:", err)
		os.Exit(1)
	}
}

func getTLSConfig() (*tls.Config, error) {
	certFile := viper.GetString("tls.cert")
	keyFile := viper.GetString("tls.key")
	caFile := viper.GetString("tls.caCert")
	if certFile == "" && keyFile == "" && caFile == "" {
		return nil, nil
	}

	// Load client cert
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}

	// Load CA cert
	caCert, err := ioutil.ReadFile(caFile)
	if err != nil {
		return nil, err
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	// Compile the TLS Config
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
	}

	tlsConfig.BuildNameToCertificate()

	return tlsConfig, nil
}

func getBrokers() ([]string, error) {
	brokerList := viper.GetStringSlice("brokers")
	if len(brokerList) == 0 {
		return nil, errors.New("no brokers specified")
	}

	return brokerList, nil
}

func getKafkaVersion() (sarama.KafkaVersion, error) {
	valueFromFile := viper.GetString("kafka_version")

	// If there is a version provided in the config file then parse it and return it
	if valueFromFile != "" {
		parsedVersion, err := sarama.ParseKafkaVersion(valueFromFile)
		if err != nil {
			return sarama.KafkaVersion{}, fmt.Errorf("error when parsing kafka version config value: %s", err)
		}
		return parsedVersion, nil
	}

	// If version is not provided then return the default v1.
	// Some features will not be available (e.g. ACL resource pattern types)
	return sarama.V1_0_0_0, nil
}

func getConfig() (*sarama.Config, error) {
	conf := sarama.NewConfig()
	version, err := getKafkaVersion()
	if err != nil {
		return nil, fmt.Errorf("could not set the kafka version: %s", err)
	}
	conf.Version = version
	conf.Consumer.Return.Errors = true
	conf.Admin.Timeout = 20 * time.Second

	tlsConfig, err := getTLSConfig()
	if err != nil {
		return nil, fmt.Errorf("cannot get TLS conf: %s", err)
	}

	if tlsConfig != nil {
		conf.Net.TLS.Enable = true
		conf.Net.TLS.Config = tlsConfig
	}

	return conf, err
}

// getKafkaClient returns a new client
func getKafkaClient() (sarama.Client, error) {
	conf, err := getConfig()
	if err != nil {
		return nil, fmt.Errorf("getKafkaClient: cannot get configuration: %v", err)
	}

	brokerList, err := getBrokers()
	if err != nil {
		return nil, err
	}

	// Create new client
	return sarama.NewClient(brokerList, conf)
}
