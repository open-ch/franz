package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v2"
	"github.com/open-ch/franz/pkg/franz"
	"github.com/open-ch/franz/pkg/list"
)

func getFranzConfig() franz.Config {
	c := franz.Config{
		KafkaVersion:   viper.GetString("kafka_version"),
		Brokers:        viper.GetStringSlice("brokers"),
		SchemaRegistry: viper.GetString("registry"),
	}

	certFile := viper.GetString("tls.cert")
	keyFile := viper.GetString("tls.key")
	caFile := viper.GetString("tls.caCert")
	if certFile != "" || keyFile != "" || caFile != "" {
		c.TLSConfig = &franz.TLSConfig{
			CertFile: certFile,
			KeyFile:  keyFile,
			CaFile:   caFile,
		}
	}

	return c
}

// execute does several things:
// 1. creates new franz instance
// 2. executes the passed in function
// 3. on success, prints the return value to the console,
//    otherwise it just returns the error
func execute(fun func(ctx context.Context, f *franz.Franz) (string, error)) error {
	ctx, cancel := context.WithCancel(context.Background())

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-signals
		cancel() // attempt for graceful shutdown after first signal
		<-signals
		os.Exit(1) // force exit after second signal
	}()

	f, err := franz.New(getFranzConfig(), verbose)
	if err != nil {
		return err
	}
	defer f.Close()

	out, err := fun(ctx, f)
	if err != nil {
		return err
	}

	if out != "" {
		fmt.Println(out)
	}

	return nil
}

func convertSliceIntToInt32(a []int) []int32 {
	var out []int32
	for i := range a {
		out = append(out, int32(i))
	}

	return out
}

func formatWithCaption(entry interface{}, allowTable bool, caption string) (string, error) {
	if allowTable && formatTable {
		return list.FormatTable(entry, caption)
	}

	if formatYAML {
		return list.FormatYAML(entry)
	}

	return list.FormatJSON(entry)
}

func format(entry interface{}, allowTable bool) (string, error) {
	return formatWithCaption(entry, allowTable, "")
}

func decode(path string, x interface{}) error {
	if path == "" {
		return errors.New("no file specified")
	}

	f, err := os.Open(path)
	if err != nil {
		return errors.Wrap(err, "failed to read file")
	}
	defer f.Close()

	d := yaml.NewDecoder(f)
	return d.Decode(x)
}
