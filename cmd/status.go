package cmd

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/open-ch/franz/pkg/franz"
)

func init() {
	statusCmd := &cobra.Command{
		Use:   "status",
		Short: "Show the status of Kafka System",
		Long: `status shows various relevant information of the Kafka brokers:
	 - Partitions
     - Topics
	 - Consumers and their Offsets
	 - Producers`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return execute(func(_ context.Context, f *franz.Franz) (s string, err error) {
				status, err := f.Status()
				if err != nil {
					return "", err
				}

				return format(status, true)
			})
		},
	}

	RootCmd.AddCommand(statusCmd)
}
