package cmd

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/open-ch/franz/pkg/franz"
)

func init() {
	var (
		aclsFile        string
		apply           bool
		includeDeletion bool
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
		Long:  `Sets Kafka ACLs from Config Files.

Note that by default only a dry run will be made, no action is taken. To apply the changes, specify --apply.
Furthermore, by default no ACLs will be deleted. To override this, specify --include-deletion. Once again,
in the dry run mode the deleted ACLs will only be displayed but not deleted, --apply is required to actually
apply the changes.
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return execute(func(_ context.Context, f *franz.Franz) (s string, err error) {
				var kafkaACLs franz.KafkaACLs
				if err := decode(aclsFile, &kafkaACLs); err != nil {
					return "", err
				}

				diff, err := f.GetACLsDiff(kafkaACLs)
				if err != nil {
					return "", err
				}

				if !includeDeletion {
					diff.ToDelete = nil
				}

				if !apply {
					return format(diff.Transform(), false)
				}

				return "", f.SetACLs(diff)
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
	setACLCmd.Flags().BoolVarP(&apply, "apply", "a", false, "Apply the changes")
	setACLCmd.Flags().BoolVarP(&includeDeletion, "include-deletion", "d", false, "Remove ACLs that should be removed")

	RootCmd.AddCommand(aclsCmd)
	aclsCmd.AddCommand(setACLCmd)
	aclsCmd.AddCommand(listACLCmd)
}
