package integ

import (
	"context"

	"github.com/ajzo90/go-requests"
)

type Proto interface {
	// Open a new stream loader. Should emit or record the schema information
	// Proto can return nil in case this stream should not be emitted
	Open(typ Schema) StreamProto

	// Close closes the current session. Flushes pending data
	Close() error

	// EmitSpec defines the available streams
	EmitSpec(ConnectorSpecification) error

	EmitStatus(v error) error // can we move this to Proto
}

type StreamProto interface {
	Load(config, state interface{}) error

	EmitBatch(ctx context.Context, req *requests.Request, resp *requests.JSONResponse, path ...string) error

	EmitState(v interface{}) error

	EmitLog(v interface{}) error
}
