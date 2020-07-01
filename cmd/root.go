package cmd

import (
	"fmt"
	"os"

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
