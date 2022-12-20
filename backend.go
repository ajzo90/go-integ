package integ

import (
	"context"
	"github.com/valyala/fastjson"
)

type ManualContext interface {
	Stream(schema Schema) (ManualStreamContext, error)
}

type ManualStreamContext interface {
	EmitValues(arr []*fastjson.Value) error
	Load(a, b any) error
	EmitState(any) error
	EmitLog(any) error
}

var _ ManualContext = &manualCtx{}
var _ ManualStreamContext = &manualStreamCtx{}

type manualCtx struct {
	p        Proto
	flushers []func() error
}

func (m *manualCtx) Close() error {
	for _, f := range m.flushers {
		if err := f(); err != nil {
			return err
		}
	}
	return nil
}

func (m *manualCtx) Stream(schema Schema) (ManualStreamContext, error) {
	sp, err := m.p.Open(schema)
	if err != nil {
		return nil, err
	}
	m.flushers = append(m.flushers, sp.Flush)
	return &manualStreamCtx{baseRunContext: makeBaseRunCtx(context.TODO(), schema, sp)}, nil
}

type manualStreamCtx struct {
	baseRunContext
}

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
