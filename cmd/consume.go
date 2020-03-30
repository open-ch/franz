package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/spf13/cast"
	"github.com/spf13/cobra"
	"SCRUBBED-URL
	"SCRUBBED-URL
)

const (
	defaultMessageCount = 25
)

func init() {
	var (
		partitions []int
		count      int64
		start      string
		duration   time.Duration
		follow     bool
		decode     bool
	)

	var monitorCmd = &cobra.Command{
		Use:     "consume [topic]",
		Aliases: []string{"monitor"},
		Short:   "Consume a specific kafka topic",
		Long: `Consume a specific kafka topic.

You may consume from a kafka topic with arbitrary offsets.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			topic := args[0]

			return execute(func(ctx context.Context, f *franz.Franz) (string, error) {
				if start != "" {
					// historical mode
					from, err := cast.StringToDate(start)
					if err != nil {
						return "", err
					}

					var to time.Time
					if duration > 0 {
						to = from.Add(duration)
					}

					req := franz.HistoryRequest{
						Topic:      topic,
						From:       from,
						To:         to,
						Partitions: convertSliceIntToInt32(partitions),
						Decode:     decode,
					}

					messages, err := f.HistoryEntries(req)
					if err != nil {
						return "", err
					}

					return format(messages, true)
				}

				// non-historical mode
				req := franz.MonitorRequest{
					Topic:      topic,
					Partitions: convertSliceIntToInt32(partitions),
					Count:      count,
					Follow:     follow,
					Decode:     decode,
				}

				rec, err := f.Monitor(req)
				if err != nil {
					return "", err
				}

				go func() {
					<-ctx.Done()
					rec.Stop()
				}()

				for {
					msg, err := rec.Next()
					if errors.Is(err, io.EOF) {
						return "", nil
					} else if err != nil {
						return "", err
					}

					out, err := list.FormatJSON(msg)
					if err != nil {
						return "", err
					}

					fmt.Println(out)
				}
			})
		},
	}

	RootCmd.AddCommand(monitorCmd)

	monitorCmd.Flags().Int64VarP(&count, "number", "n", defaultMessageCount, "Consumes the n last messages for each partition")
	monitorCmd.Flags().IntSliceVarP(&partitions, "partitions", "p", nil, "The partitions to consume (comma-separated), all partitions will be used if not set")
	monitorCmd.Flags().DurationVarP(&duration, "duration", "d", 0, "Time-frame after \"from\", only effective with -s")
	monitorCmd.Flags().StringVarP(&start, "start", "s", "", "Starting time, disables -f and -n")
	monitorCmd.Flags().BoolVarP(&follow, "follow", "f", false, "Consume future messages when they arrive")
	monitorCmd.Flags().BoolVar(&decode, "decode", false, "Decodes the message according to the schema defined in the schema registry")
}
