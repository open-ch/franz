package franz

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockRegistry struct {
	schema string
}

func (m *mockRegistry) SchemaByID(_ uint32) (string, error) {
	return m.schema, nil
}

func TestOther(t *testing.T) {
	tests := []struct {
		input  string
		schema string
	}{
		{
			input: `{"ID": 5, "Name": "JSON Data"}`,
			schema: `{
				"type": "record",
				"name": "Example",
				"fields": [
					{"name": "ID", "type": "int"},
					{"name": "Name", "type": "string"}
					]
				}`,
		},
		{
			input:  `"Some string message"`,
			schema: `"string"`,
		},
	}

	registry := mockRegistry{}
	encoder := newAvroCodec(&registry)
	for _, test := range tests {
		registry.schema = test.schema

		out, err := encoder.Encode([]byte(test.input), 0)
		require.NoError(t, err)

		out, err = encoder.Decode(out)
		require.NoError(t, err)

		assert.JSONEq(t, test.input, string(out))
	}
}
