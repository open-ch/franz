package cmd

import (
	"fmt"
	"io"
	"log/syslog"
	"os"

	"github.com/sirupsen/logrus"
)

func getLogger() *logrus.Logger {
	log := logrus.New()
	sysWriter, err := syslog.New(syslog.LOG_INFO, "franz")
	if err != nil {
		fmt.Printf("cannot get logger: %s\n", err)
		os.Exit(1)
	}

	log.Level = logrus.WarnLevel
	if verbose {
		log.Level = logrus.DebugLevel
	}

	log.Out = io.MultiWriter(os.Stderr, sysWriter)

	return log
}
