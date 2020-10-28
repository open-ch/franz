package franz

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"time"

	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
)

func createLogger(verbose bool) (*logrus.Logger, error) {
	log := logrus.New()

	log.Level = logrus.ErrorLevel
	if verbose {
		log.Level = logrus.DebugLevel
	}

	return log, nil
}

func parseConfig(c Config) (*sarama.Config, error) {
	sc := sarama.NewConfig()

	sc.Version = sarama.V1_0_0_0
	if c.KafkaVersion != "" {
		v, err := sarama.ParseKafkaVersion(c.KafkaVersion)
		if err != nil {
			return nil, err
		}

		sc.Version = v
	}

	sc.Consumer.Return.Errors = true
	sc.Producer.Return.Successes = true
	sc.Admin.Timeout = 20 * time.Second

	if c.TLSConfig != nil {
		tlsConfig, err := c.TLSConfig.loadTLSConfig()
		if err != nil {
			return nil, err
		}

		sc.Net.TLS.Enable = true
		sc.Net.TLS.Config = tlsConfig
	}

	return sc, nil
}

func (c TLSConfig) loadTLSConfig() (*tls.Config, error) {
	// Load client cert
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, err
	}

	// Load CA cert
	caCert, err := ioutil.ReadFile(c.CaFile)
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
