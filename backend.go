package integ

import (
	"github.com/valyala/fastjson"
)

type Proto interface {
	// Open a new stream loader. Should emit or record the schema information
	// Proto can return nil in case this stream should not be emitted
	Open(typ Schema) (StreamProto, error)

	// Close closes the current session. Flushes pending data
	Close() error

	// EmitSpec defines the available streams
	EmitSpec(ConnectorSpecification) error

	EmitStatus(v error) error // can we move this to Proto
}

type StreamProto interface {
	Load(config, state interface{}) error

	EmitValues(arr []*fastjson.Value) error

	EmitState(v interface{}) error

	EmitLog(v interface{}) error

	Flush() error
}
