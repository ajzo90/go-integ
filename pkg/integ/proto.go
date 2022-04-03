package integ

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/ajzo90/go-jsonschema-generator"
	"github.com/ajzo90/go-requests"
	"github.com/valyala/fastjson"
	"golang.org/x/sync/errgroup"
	"io"
	"net/http"
	"runtime/debug"
	"sort"
	"strings"
)

type Proto interface {
	Open(name string, typ interface{}) ExtendedStreamLoader
	Spec(interface{}) error
	Close() error
	Streams() Streams
}

type ExtendedStreamLoader interface {
	StreamLoader
	Schema(v interface{}) error
	Log(v interface{}) error
	Status(v error) error
}

// Loader is what the concrete integration can access
type StreamLoader interface {
	Load(config, state interface{}) error
	WriteBatch(ctx context.Context, q *requests.Request, keys ...string) (*requests.JSONResponse, error)
	State(v interface{}) error
	Fields() []string
}

type Stream struct {
	Name string
}
type Streams []Stream

func (streams Streams) Contains(name string) bool {
	isOk := len(streams) == 0
	for _, st := range streams {
		isOk = isOk || st.Name == name
	}
	return isOk
}

type Settings struct {
	Format  string
	Streams Streams
}

func Open(r io.Reader, w io.Writer) (Proto, error) {

	var p fastjson.Parser
	var i = &integration{states: map[string][]byte{}, _w: w}
	var buf []byte

	var marshal = func(v *fastjson.Value) []byte {
		if v == nil {
			return nil
		}
		var out []byte
		buf = v.MarshalTo(buf[:0])
		return append(out, buf...)
	}

	var scanner = bufio.NewScanner(r)
	for scanner.Scan() {
		var b = scanner.Bytes()
		if len(b) == 0 {
			continue
		}
		v, err := p.ParseBytes(b)
		if err != nil {
			return nil, err
		}
		switch t := string(v.GetStringBytes("type")); t {
		case "SETTINGS":
			b := marshal(v.Get("settings"))
			if err := json.NewDecoder(bytes.NewReader(b)).Decode(&i.settings); err != nil {
				return nil, err
			}
		case "CONFIG":
			i.config = marshal(v.Get("config"))
		case "STATE":
			var stream = string(v.GetStringBytes("stream"))
			i.states[stream] = marshal(v.Get("state"))
		default:
			return nil, fmt.Errorf("invalid type '%s'", t)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	switch i.settings.Format {
	case "mock", "airbyte":
		return &airbyteProto{integration: i}, nil
	default:
		return nil, fmt.Errorf("not supported")
	}
}

func newWrap(typ string, stream string) *fastjson.Value {
	var a fastjson.Arena
	var o = a.NewObject()
	o.Set("type", a.NewString(typ))
	o.Set("stream", a.NewString(stream))
	return o
}

func Keys(schema *jsonschema.Document) []string {
	var o []string
	for k := range schema.Properties {
		o = append(o, k)
	}
	sort.Strings(o)
	return o
}

type RunnerFunc func(ctx context.Context, pw StreamLoader) error

func (r RunnerFunc) Run(ctx context.Context, pw StreamLoader) error {
	return r(ctx, pw)
}

type MaskedString string

func (s MaskedString) String() string {
	return string(s)
}

func (s MaskedString) Masked() string {
	return strings.Repeat("x", len(s))
}

func (s MaskedString) MarshalJSON() ([]byte, error) {
	return []byte(s.Masked()), nil
}

type Runner interface {
	// Run runs the sync job
	Run(ctx context.Context, loader StreamLoader) error
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

func (r *runner) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	var p = strings.Split(request.URL.Path, "/")
	var last = p[len(p)-1]

	if err := r.Handle(request.Context(), writer, last, request.Body); err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	}
}

func (r *runner) Handle(ctx context.Context, writer io.Writer, path string, rd io.Reader) error {
	proto, err := Open(rd, writer)
	if err != nil {
		return err
	}

	err = r.handle(ctx, proto, path)
	closeErr := proto.Close()

	if err != nil {
		return err
	} else {
		return closeErr
	}
}

func (r *runner) handle(ctx context.Context, proto Proto, path string) error {
	switch path {
	case "spec":
		return r.Spec(ctx, proto)
	case "validate", "check":
		return r.Check(ctx, proto)
	case "discover":
		return r.Run(ctx, proto, false)
	case "read":
		return r.Run(ctx, proto, true)
	default:
		return fmt.Errorf("invalid path")
	}
}

func NewLoader(config interface{}) *runner {
	return &runner{runners: map[string]runnerTyp{}, config: config}
}

func (r *runner) Stream(name string, runner Runner, typ interface{}) *runner {
	r.runners[name] = runnerTyp{typ: typ, fn: runner}
	return r
}

type panicErr string

func (p panicErr) Error() string {
	return string(p)
}

type validatorLoader struct {
	ExtendedStreamLoader
}

func (m *validatorLoader) WriteBatch(ctx context.Context, q *requests.Request, keys ...string) (*requests.JSONResponse, error) {
	if _, err := q.ExecJSON(ctx); err != nil {
		return nil, err
	} else {
		return nil, validatorOK
	}
}

var validatorOK = fmt.Errorf("validatorOK")

func (r *runner) Check(ctx context.Context, proto Proto) error {
	for stream, runner := range r.runners {
		pw := proto.Open(stream, runner.typ)
		if err := runner.fn.Run(ctx, &validatorLoader{ExtendedStreamLoader: pw}); err == validatorOK {
			return pw.Status(nil)
		} else if err != nil {
			return pw.Status(fmt.Errorf("validation failed: %s", err.Error()))
		}
	}
	return fmt.Errorf("validation failed: unexpected error")
}

func (r *runner) Run(ctx context.Context, proto Proto, sync bool) error {
	var streams = proto.Streams()
	wg, ctx := errgroup.WithContext(ctx)
	for stream, runner := range r.runners {
		if !streams.Contains(stream) {
			continue
		}
		stream, runner := stream, runner // copy
		wg.Go(func() (err error) {
			return run(ctx, proto, stream, runner, sync)
		})
	}
	return wg.Wait()
}

func run(ctx context.Context, proto Proto, stream string, runner runnerTyp, sync bool) (err error) {
	pw := proto.Open(stream, runner.typ)
	defer func() {
		if pErr := recover(); pErr != nil {
			err = panicErr(debug.Stack())
		}
		if err != nil {
			err = pw.Log(err)
		}
	}()

	if err := pw.Schema(runner.typ); err != nil {
		return err
	} else if sync {
		return runner.fn.Run(ctx, pw)
	}
	return nil
}

func (r *runner) Spec(ctx context.Context, proto Proto) error {
	type ConnectorSpecification struct {
		DocumentationURL        string               `json:"documentationUrl,omitempty"`
		SupportsIncremental     bool                 `json:"supportsIncremental"`
		ConnectionSpecification *jsonschema.Document `json:"connectionSpecification"`
	}

	return proto.Spec(ConnectorSpecification{
		DocumentationURL:        "127.0.0.1/docs",
		SupportsIncremental:     true, // why is this important to share?
		ConnectionSpecification: jsonschema.New(r.config),
	})
}
