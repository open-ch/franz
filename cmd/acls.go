package cmd

import (
	"context"

	"github.com/spf13/cobra"
	"SCRUBBED-URL
)

func init() {
	var (
		aclsFile      string
		forceDeletion bool
		apply         bool
	)

	// aclsCmd represents the acls command
	var aclsCmd = &cobra.Command{
		Use:   "acls",
		Short: "List and Set Kafka ACLs",
		Long:  `List and Set Kafka ACLs`,
	}

	// setACLCmd represents the acls set command
	var setACLCmd = &cobra.Command{
		Use:   "set",
		Short: "Sets Kafka ACLs from Config Files",
		Long:  `Sets Kafka ACLs from Magic Config Files`,
		RunE: func(cmd *cobra.Command, args []string) error {
			var kafkaACLs franz.KafkaACLs
			if err := decode(aclsFile, &kafkaACLs); err != nil {
				return err
			}

			return execute(func(_ context.Context, f *franz.Franz) (s string, err error) {
				if !apply {
					changes, err := f.SetACLsDryRun(kafkaACLs)
					if err != nil {
						return "", err
					}

					return format(changes, false)
				}

				return "", f.SetACLs(kafkaACLs, forceDeletion)
			})
		},
	}

	// listACLCmd represents the acls list command
	var listACLCmd = &cobra.Command{
		Use:   "list",
		Short: "List Kafka ACLs",
		Long:  `List Kafka ACLs`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return execute(func(_ context.Context, f *franz.Franz) (s string, err error) {
				acls, err := f.GetAcls()
				if err != nil {
					return "", err
				}

				return format(acls, false)
			})
		},
	}

	// Flags
	setACLCmd.Flags().StringVarP(&aclsFile, "file", "f", "", "File containing the acls to set")
	setACLCmd.Flags().BoolVar(&forceDeletion, "force", false, "Force deletion of ACLs that are not found on the config file")
	setACLCmd.Flags().BoolVarP(&apply, "apply", "a", false, "Apply the changes")

	RootCmd.AddCommand(aclsCmd)
	aclsCmd.AddCommand(setACLCmd)
	aclsCmd.AddCommand(listACLCmd)
}
