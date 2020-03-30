package cmd

import (
	"context"

	"github.com/spf13/cobra"
	"SCRUBBED-URL
)

func init() {
	var registryCmd = &cobra.Command{
		Use:   "registry",
		Short: "Interact with the schema registry",
	}

	var listCmd = &cobra.Command{
		Use:   "list",
		Short: "List all registered schema subjects",
		RunE: func(cmd *cobra.Command, args []string) error {
			return execute(func(_ context.Context, f *franz.Franz) (s string, err error) {
				subjects, err := f.Registry().Subjects()
				if err != nil {
					return "", err
				}

				return format(subjects, false)
			})
		},
	}

	var getCmd = &cobra.Command{
		Use:   "get [subject]",
		Short: "Get information about the schema for the given subject",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return execute(func(_ context.Context, f *franz.Franz) (s string, err error) {
				subjects, err := f.Registry().SchemaBySubject(args[0])
				if err != nil {
					return "", err
				}

				return format(subjects, false)
			})
		},
	}

	registryCmd.AddCommand(listCmd, getCmd)
	RootCmd.AddCommand(registryCmd)
}
