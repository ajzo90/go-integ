package integ

import (
	"github.com/ajzo90/go-requests"
	"github.com/valyala/fastjson"
)

type HttpRunner interface {
	// Run runs the sync job.
	Run(ctx HttpContext) error
}

type ManualRunner interface {
	Run(ctx ManualContext) error
}

type DbRunner interface {
	Run(ctx DbContext) error
}

type FsRunner interface {
	Run(ctx FsContext) error
}

type GeneralRunner interface {
	Run(ctx GeneralContext) error
}

type GeneralContext interface {
	// Load a stream with shared config and state
	Load(config, state interface{}) error

	Schema() Schema

	// EmitState emit the state
	EmitState(v interface{}) error

	EmitLog(v any) error

	EmitValues(v []*fastjson.Value) error

	EmitValue(v any) error

	//EmitRelated(typ Schema, v any) error
}

type HttpContext interface {
	GeneralContext

	// EmitBatch executes the provided request, locate the data array and emit the records
	// (likely) called multiple times in the same run
	// resp: (pre-allocated and reusable)
	// path: (path to the data array)
	EmitBatch(req *requests.Request, resp *requests.JSONResponse, keys ...string) error
}

type DbContext interface {
	GeneralContext
}

type FsContext interface {
	GeneralContext
}
