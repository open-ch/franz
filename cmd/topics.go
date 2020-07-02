package cmd

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/open-ch/franz/pkg/franz"
)

func init() {
	var (
		topicsFile      string
		apply           bool
		includeDeletion bool
		includeInternal bool
	)

	var topicsCmd = &cobra.Command{
		Use:   "topics",
		Short: "Manage topics",
		Long:  "Create and configure topics",
	}

	var setTopicsCmd = &cobra.Command{
		Use:   "set",
		Short: "Sets Kafka topics from config files",
		Long: `Sets Kafka topics from a config file

Note that by default only a dry run will be made, no action is taken. To apply the changes, specify --apply.
Furthermore, by default no topics will be deleted. To override this, specify --include-deletion. Once again,
in the dry run mode the deleted topics will only be displayed but not deleted, --apply is required to actually
apply the changes.
Note that internal topics will not be modified.
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			var topicWrapper TopicWrapper
			if err := decode(topicsFile, &topicWrapper); err != nil {
				return err
			}

			return execute(func(_ context.Context, f *franz.Franz) (s string, err error) {
				diff, err := f.GetTopicsDiff(topicWrapper.Topics)
				if err != nil {
					return "", err
				}

				if !includeDeletion {
					diff.ToDelete = nil
				}

				if !apply {
					return format(diff, false)
				}

				return "", f.SetTopics(diff)
			})
		},
	}

	var listTopicsCmd = &cobra.Command{
		Use:   "list",
		Short: "List Kafka topics",
		Long: `List Kafka topics

By default, internal topics will not be returned.
This can be overridden by specifying the --internal flag.
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return execute(func(_ context.Context, f *franz.Franz) (s string, err error) {
				topics, err := f.GetTopicsExisting(includeInternal)
				if err != nil {
					return "", err
				}

				return format(TopicWrapper{Topics: topics}, false)
			})
		},
	}

	setTopicsCmd.Flags().StringVarP(&topicsFile, "file", "f", "", "File containing the topics in YAML format to set")
	setTopicsCmd.Flags().BoolVarP(&apply, "apply", "a", false, "Apply the changes")
	setTopicsCmd.Flags().BoolVarP(&includeDeletion, "include-deletion", "d", false, "Remove topics that should be removed")
	listTopicsCmd.Flags().BoolVarP(&includeInternal, "internal", "i", false, "Also output internal topics")

	RootCmd.AddCommand(topicsCmd)
	topicsCmd.AddCommand(setTopicsCmd, listTopicsCmd)
}

type TopicWrapper struct {
	Topics []franz.Topic
}
