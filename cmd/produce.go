package cmd

import (
	"bufio"
	"context"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/open-ch/franz/pkg/franz"
)

func init() {
	var (
		key string
		encode string
	)

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

				var schemaID uint32
				if encode != "" {
					subjects, err := f.Registry().SchemaBySubject(encode)
					if err != nil {
						return "", err
					}
					schemaID = uint32(subjects.ID)
				}

				reader := bufio.NewReader(os.Stdin)
				for {
					line, err := reader.ReadString('\n')
					if err == io.EOF {
						return "", nil
					} else if err != nil {
						return "", err
					}

					line = strings.TrimSuffix(line, "\n")

					if encode != "" {
						if err := producer.SendMessageEncoded(topic, line, key, schemaID); err != nil {
							return "", err
						}
					} else {
						if err := producer.SendMessage(topic, line, key); err != nil {
							return "", err
						}
					}
				}
			})
		},
	}

	produceCmd.Flags().StringVarP(&key, "key", "k", "", "Specifies the key that should be used")
	produceCmd.Flags().StringVarP(&encode, "encode", "e", "", "Avro encoding schema name")

	RootCmd.AddCommand(produceCmd)
}
