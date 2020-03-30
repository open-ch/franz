package franz

import (
	"encoding/binary"

	"github.com/linkedin/goavro/v2"
)

type schemaSource interface {
	SchemaByID(uint32) (string, error)
}

// avroCodec en- and decodes data as defined here:
// https://docs.confluent.io/current/schema-registry/serializer-formatter.html#wire-format
type avroCodec struct {
	registry schemaSource
}

func newAvroCodec(s schemaSource) *avroCodec {
	return &avroCodec{registry: s}
}

// Decode decodes the msg according to the confluent specific schema registry encoding.
// First, it identifies the schema ID contained in the first 5 bytes. Secondly, it
// fetches the schema from the schema registry. Lastly, it decodes the rest of the
// message using the schema.
func (d *avroCodec) Decode(msg []byte) ([]byte, error) {
	schemaID := binary.BigEndian.Uint32(msg[1:5])
	schema, err := d.registry.SchemaByID(schemaID)
	if err != nil {
		return nil, err
	}

	c, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, err
	}

	out, _, err := c.NativeFromBinary(msg[5:])
	if err != nil {
		return nil, err
	}

	return c.TextualFromNative(nil, out)
}

// Encode encodes the msg according the specified schema ID. First, it fetches the

// schema from the schema registry. Secondly, it encodes the message according to
// the schema. Lastly, it prepends the schema ID to the message such that it can
// be decoded again.
func (d *avroCodec) Encode(msg []byte, schemaID uint32) ([]byte, error) {
	schema, err := d.registry.SchemaByID(schemaID)
	if err != nil {
		return nil, err
	}

	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, err
	}

	native, _, err := codec.NativeFromTextual(msg)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, 5)
	binary.BigEndian.PutUint32(buf[1:5], schemaID)

	out, err := codec.BinaryFromNative(buf, native)
	if err != nil {
		return nil, err
	}

	return out, nil
}
