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
	"sort"
	"strings"
	"sync"
)

type Proto interface {
	Open(name string, typ interface{}) ExtendedLoader
}

type Settings struct {
	Format string
}
type integration struct {
	settings Settings
	config   []byte
	states   map[string][]byte
	_w       io.Writer
	wMtx     sync.Mutex
}

func (i *integration) Write(b []byte) error {
	i.wMtx.Lock()
	defer i.wMtx.Unlock()
	_, err := i._w.Write(b)
	return err
}

func (i *integration) Load(stream string, config, state interface{}) error {
	if len(i.config) > 0 {
		if err := json.NewDecoder(bytes.NewReader(i.config)).Decode(config); err != nil {
			return err
		}
	} else if config != nil {
		return fmt.Errorf("expected config")
	}

	if v := i.states[stream]; len(v) == 0 {
		return nil
	} else if err := json.NewDecoder(bytes.NewReader(v)).Decode(state); err != nil {
		return err
	}
	return nil
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
	case "mock":
		return &mockIntegration{integration: i}, nil
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

type ExtendedLoader interface {
	Loader
	Schema(v interface{}) error
}

type Loader interface {
	Load(config, state interface{}) error
	WriteBatch(ctx context.Context, q *requests.Request, keys ...string) (*requests.JSONResponse, error)
	State(v interface{}) error
	Fields() []string
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

func (r *runner) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	var p = strings.Split(request.URL.Path, "/")
	var last = p[len(p)-1]

	if err := r.Handle(request.Context(), writer, last, request.Body); err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	}
}

func (r *runner) Handle(ctx context.Context, writer io.Writer, path string, rd io.Reader) error {
	switch path {
	case "spec":
		// no config for this
		type ConnectorSpecification struct {
			DocumentationURL        string               `json:"documentationUrl,omitempty"`
			SupportsIncremental     bool                 `json:"supportsIncremental"`
			ConnectionSpecification *jsonschema.Document `json:"connectionSpecification"`
		}

		return json.NewEncoder(writer).Encode(ConnectorSpecification{
			DocumentationURL:        "127.0.0.1/docs",
			SupportsIncremental:     true, // why is this important to share?
			ConnectionSpecification: jsonschema.New(r.config),
		})
	case "validate", "check":
		return nil
	case "discover":
		return r.openAndRun(ctx, rd, writer, false)
	case "read":
		return r.openAndRun(ctx, rd, writer, true)
	default:
		return fmt.Errorf("invalid path")
	}
}

type LoaderThing interface {
	http.Handler
}

func NewLoader(config interface{}) *runner {
	return &runner{runners: map[string]runnerTyp{}, config: config}
}

func (r *runner) Stream(name string, runner Runner, typ interface{}) *runner {
	r.runners[name] = runnerTyp{typ: typ, fn: runner}
	return r
}

func (r *runner) openAndRun(ctx context.Context, rd io.Reader, writer io.Writer, sync bool) error {
	p, err := Open(rd, writer)
	if err != nil {
		return err
	}
	return r.Run(ctx, p, sync)
}

func (r *runner) Run(ctx context.Context, proto Proto, sync bool) error {
	wg, ctx := errgroup.WithContext(ctx)
	for stream, runner := range r.runners {
		runner := runner
		pw := proto.Open(stream, runner.typ)

		wg.Go(func() error {
			if err := pw.Schema(runner.typ); err != nil {
				return err
			} else if sync {
				return runner.fn.Run(ctx, pw)
			}
			return nil
		})
	}
	return wg.Wait()
}
