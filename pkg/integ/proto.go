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
	"log"
	"net/http"
	"runtime/debug"
	"sort"
	"strings"
)

type Runner interface {
	// Run runs the sync job.
	Run(ctx context.Context, loader StreamLoader) error
}

// StreamLoader is what the concrete integration can access
type StreamLoader interface {
	// Load a stream with config(shared) and state
	Load(config, state interface{}) error

	// WriteBatch emit records from a prepared http request
	// (probably) called multiple times
	WriteBatch(ctx context.Context, req *requests.Request, resp *requests.JSONResponse, keys ...string) error

	// State emit the state
	State(v interface{}) error

	Schema() Schema
}

type Proto interface {
	// Open a new stream loader
	Open(typ Schema) ExtendedStreamLoader

	// Spec defines the available streams
	Spec(ConnectorSpecification) error

	// Close closes the current session
	Close() error

	// Streams defines the active streams. 0 streams disable the filtering.
	Streams() Streams
}

type ExtendedStreamLoader interface {
	StreamLoader

	// WriteSchema emit the schema
	WriteSchema(v Schema) error

	// Log something
	Log(v interface{}) error

	// Status report whether the credentials/config are correct, internally it is making one http request to check
	Status(v error) error
}

type RunnerFunc func(ctx context.Context, pw StreamLoader) error

func (r RunnerFunc) Run(ctx context.Context, pw StreamLoader) error {
	return r(ctx, pw)
}

type Streams []Schema

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

func Open(r io.Reader, w io.Writer, cmd cmd, protos Protos) (Proto, error) {

	var p fastjson.Parser
	var i = &Protocol{states: map[string][]byte{}, _w: w, cmd: cmd}
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

	fn, ok := protos[i.settings.Format]
	if !ok {
		return nil, fmt.Errorf("not supported")
	}
	return fn(i), nil
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

type runners []runnerTyp
type runnerTyp struct {
	fn     Runner
	schema Schema
}

type runner struct {
	config  interface{}
	runners runners
	protos  Protos
}

func (r *runner) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	var p = strings.Split(request.URL.Path, "/")
	var last = p[len(p)-1]

	if err := r.Handle(request.Context(), cmd(last), writer, request.Body, r.protos); err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	}
}

type cmd string

const (
	cmdSpec     cmd = "spec"
	cmdCheck    cmd = "check"
	cmdDiscover cmd = "discover"
	cmdRead     cmd = "read"
)

type ProtoFn func(protocol *Protocol) Proto
type Protos map[string]ProtoFn

func (r *runner) Handle(ctx context.Context, cmd cmd, writer io.Writer, rd io.Reader, protos Protos) error {
	proto, err := Open(rd, writer, cmd, protos)
	if err != nil {
		return err
	}

	err = r.handle(ctx, proto, cmd)
	closeErr := proto.Close()

	if err != nil {
		return err
	} else {
		return closeErr
	}
}

func (r *runner) Protos(protos Protos) {
	r.protos = protos
}

func (r *runner) handle(ctx context.Context, proto Proto, cmd cmd) error {
	switch cmd {
	case cmdSpec:
		return r.Spec(ctx, proto)
	case cmdCheck:
		return r.Check(ctx, proto)
	case cmdDiscover:
		return r.Run(ctx, proto, false)
	case cmdRead:
		return r.Run(ctx, proto, true)
	default:
		return fmt.Errorf("invalid path")
	}
}

func NewLoader(config interface{}) *runner {
	return &runner{config: config}
}

type Loader interface {
	http.Handler
	Validate() error
	Protos(Protos)
}

func (r *runner) Add(schema SchemaBuilder, runner Runner) *runner {
	r.runners = append(r.runners, runnerTyp{schema: schema.Schema, fn: runner})
	return r
}

type panicErr string

func (p panicErr) Error() string {
	return string(p)
}

type validatorLoader struct {
	ExtendedStreamLoader
}

func (m *validatorLoader) WriteBatch(ctx context.Context, req *requests.Request, resp *requests.JSONResponse, keys ...string) error {
	if err := req.Extended().ExecJSONPreAlloc(resp, ctx); err != nil {
		return err
	} else {
		return validatorOK
	}
}

var validatorOK = fmt.Errorf("validatorOK")

func (r *runner) Validate() error {
	for _, runner := range r.runners {
		if err := runner.schema.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func (r *runner) Check(ctx context.Context, proto Proto) error {
	for _, runner := range r.runners {
		pw := proto.Open(runner.schema)
		if err := runner.fn.Run(ctx, &validatorLoader{ExtendedStreamLoader: pw}); err == validatorOK {
			return pw.Status(nil)
		} else if err != nil {
			return pw.Status(fmt.Errorf("validation failed: %s", err.Error()))
		}
	}
	return fmt.Errorf("validation failed: unexpected error")
}

func (r *runner) Discover(ctx context.Context, proto Proto) error {
	return r.Run(ctx, proto, false)
}

type ConnectorSpecification struct {
	DocumentationURL        string               `json:"documentationUrl,omitempty"`
	SupportsIncremental     bool                 `json:"supportsIncremental"`
	ConnectionSpecification *jsonschema.Document `json:"connectionSpecification"`
}

func (r *runner) Spec(ctx context.Context, proto Proto) error {

	return proto.Spec(ConnectorSpecification{
		DocumentationURL:        "127.0.0.1/docs",
		SupportsIncremental:     true, // why is this important to share?
		ConnectionSpecification: jsonschema.New(r.config),
	})
}

func (r *runner) Read(ctx context.Context, proto Proto) error {
	return r.Run(ctx, proto, true)
}

func (r *runner) Run(ctx context.Context, proto Proto, sync bool) error {
	var streams = proto.Streams()
	wg, ctx := errgroup.WithContext(ctx)
	for _, runner := range r.runners {
		runner := runner // copy

		if !streams.Contains(runner.schema.Name) {
			continue
		}

		wg.Go(func() (err error) {
			return run(ctx, proto, runner, sync)
		})
	}
	return wg.Wait()
}

func run(ctx context.Context, proto Proto, runner runnerTyp, sync bool) (err error) {
	pw := proto.Open(runner.schema)
	defer func() {
		if pErr := recover(); pErr != nil {
			var s = debug.Stack()
			log.Println(string(s))
			err = panicErr(s)
		}
		if err != nil {
			err = pw.Log(err)
		}
	}()

	if err := pw.WriteSchema(runner.schema); err != nil {
		return err
	} else if sync {
		return runner.fn.Run(ctx, pw)
	}
	return nil
}
