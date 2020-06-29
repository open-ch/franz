// Package list provides a utility function to quickly print a table based on the
// element fields of a slice. This package is currently used for franz - while it
// can, in theory, be used for other packages, there are still various edge cases
// that should be taken care of, such as passing in something different than
// a slice or a slice of interface{}.
package list

import (
	"reflect"
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"
)

const tagName = "header"

// FormatTable takes a slice with a distinct type as argument and prints it as a table,
// one row for for each entry in the slice. The columns are based on the statically declared
// element type of the slice, so note that passing in e.g. []interface{} does not make much sense.
// By default, the field name is taken as the column name. This can be overridden by setting a tag
// like so: `header:"New Column Name"`.
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

	case reflect.Bool:
		if val.Bool() {
			return "true"
		}

		return "false"

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
