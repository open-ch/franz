package list

import (
	"encoding/json"
	"reflect"
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"
	"gopkg.in/yaml.v2"
)

const tagName = "header"

func FormatJSON(entry interface{}) (string, error) {
	out, err := json.MarshalIndent(entry, "", "  ")
	if err != nil {
		return "", nil
	}

	return string(out), nil
}

func FormatYAML(entry interface{}) (string, error) {
	out, err := yaml.Marshal(entry)
	if err != nil {
		return "", nil
	}

	return string(out), nil
}

func FormatTable(entries interface{}, caption string) (string, error) {
	var builder strings.Builder
	table := tablewriter.NewWriter(&builder)
	table.SetAlignment(tablewriter.ALIGN_LEFT)

	table.SetHeader(getHeaderNames(entries))
	table.AppendBulk(getRows(entries))
	if caption != "" {
		table.SetCaption(true, caption)
	}
	table.Render()

	return builder.String(), nil
}

func getHeaderNames(i interface{}) []string {
	t := reflect.TypeOf(i).Elem()

	var tags []string
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tag := field.Tag.Get(tagName)

		if tag != "" {
			tags = append(tags, tag)
		} else {
			tags = append(tags, field.Name)
		}
	}

	return tags
}

func getRows(e interface{}) [][]string {
	t := reflect.ValueOf(e)

	var rows [][]string
	for i := 0; i < t.Len(); i++ {
		row := t.Index(i)

		var rowString []string
		for idx := 0; idx < row.NumField(); idx++ {
			val := row.Field(idx)
			rowString = append(rowString, toString(val))
		}

		rows = append(rows, rowString)
	}

	return rows
}

func toString(val reflect.Value) string {
	switch val.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(val.Int(), 10)
	case reflect.Slice:
		var elems []string
		for i := 0; i < val.Len(); i++ {
			elems = append(elems, toString(val.Index(i)))
		}

		return strings.Join(elems, ", ")
	default:
		return val.String()
	}
}
