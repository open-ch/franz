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

By default, only the diff will be printed. To actually perform the changes use the --apply flag.
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
	listTopicsCmd.Flags().BoolVarP(&includeInternal, "internal", "i", false, "Also output internal topics")

	RootCmd.AddCommand(topicsCmd)
	topicsCmd.AddCommand(setTopicsCmd, listTopicsCmd)
}

type TopicWrapper struct {
	Topics []franz.Topic
}
