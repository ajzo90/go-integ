package integ

import (
	"context"
	"github.com/ajzo90/go-jsonschema-generator"
	"github.com/ajzo90/go-requests"
	"github.com/valyala/fastjson"
	"golang.org/x/sync/errgroup"
	"sort"
)

type Proto interface {
	Open(name string) StreamWriter
}

type Loader interface {
	Load(config, state interface{}) error
	WriteBatch(ctx context.Context, q *requests.Request, keys ...string) (*requests.JSONResponse, error)
	State(v interface{}) error
}

type StreamWriter interface {
	Loader
	Write(record *fastjson.Value) error
	Schema(v interface{})
	Log(v interface{})
	Close() error
}

func Keys(schema *jsonschema.Document) []string {
	var o []string
	for k := range schema.Properties {
		o = append(o, k)
	}
	sort.Strings(o)
	return o
}

type RunnerFunc func(ctx context.Context, pw Loader) error

func (r RunnerFunc) Run(ctx context.Context, pw Loader) error {
	return r(ctx, pw)
}

type Runner interface {
	Run(ctx context.Context, loader Loader) error
}

type runners map[string]runnerTyp
type runnerTyp struct {
	fn  Runner
	typ interface{}
}

type runner struct {
	config  interface{}
	runners runners
}

func NewLoader(config interface{}) *runner {
	return &runner{runners: map[string]runnerTyp{}, config: config}
}

func (r *runner) Stream(name string, runner Runner, typ interface{}) *runner {
	r.runners[name] = runnerTyp{typ: typ, fn: runner}
	return r
}

func Run(ctx context.Context, proto Proto, runners runners) error {
	wg, ctx := errgroup.WithContext(ctx)
	for stream, runner := range runners {
		pw := proto.Open(stream)
		pw.Schema(runner.typ)
		wg.Go(func() error {
			return runner.fn.Run(ctx, pw)
		})
	}
	return wg.Wait()
}

func NextBatch(ctx context.Context, pw StreamWriter, q *requests.Request, keys ...string) (*requests.JSONResponse, error) {
	pw.Log("next start")
	resp, err := q.ExecJSON(context.WithValue(ctx, "xx", "yy"))
	if err != nil {
		return nil, err
	}
	for _, v := range resp.GetArray(keys...) {
		if err := pw.Write(v); err != nil {
			return nil, err
		}
	}
	pw.Log("next done")
	return resp, err
}
