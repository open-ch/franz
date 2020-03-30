package list

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockEntry struct {
	ID    int64
	Title string
	Value string `header:"Message"`
}

func Test_getHeaderNames(t *testing.T) {
	var in []mockEntry
	out := getHeaderNames(in)
	assert.Equal(t, []string{"ID", "Title", "Message"}, out)
}

func Test_getRows(t *testing.T) {
	in := []mockEntry{
		{1, "Pumpkin", "Pie"},
		{2, "Chocolate", "Cake"},
	}
	out := getRows(in)
	assert.Equal(t, [][]string{
		{"1", "Pumpkin", "Pie"},
		{"2", "Chocolate", "Cake"},
	}, out)
}
