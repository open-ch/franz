package cmd

import (
	"bufio"
	"context"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"SCRUBBED-URL
)

func init() {
	var key string

	var produceCmd = &cobra.Command{
		Use:   "produce [topic]",
		Short: "Produce messages in the specified topic.",
		Long: `Produce messages in the specified topic.
Press Ctrl+D to exit.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			topic := args[0]

			return execute(func(_ context.Context, f *franz.Franz) (s string, err error) {
				producer, err := f.NewProducer()
				if err != nil {
					return "", err
				}
				defer producer.Close()

				reader := bufio.NewReader(os.Stdin)
				for {
					line, err := reader.ReadString('\n')
					if err == io.EOF {
						return "", nil
					} else if err != nil {
						return "", err
					}

					line = strings.TrimSuffix(line, "\n")

					if err := producer.SendMessage(topic, line, key); err != nil {
						return "", err
					}
				}
			})
		},
	}

	produceCmd.Flags().StringVarP(&key, "key", "k", "", "Specifies the key that should be used")

	RootCmd.AddCommand(produceCmd)
}
