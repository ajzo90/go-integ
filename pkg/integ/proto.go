package integ

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"runtime/debug"
	"sort"
	"strings"

	"github.com/ajzo90/go-jsonschema-generator"
	"github.com/ajzo90/go-requests"
	"github.com/valyala/fastjson"
	"golang.org/x/sync/errgroup"
)

type Runner interface {
	// Run runs the sync job.
	Run(ctx context.Context, loader Extractor) error
}

// Extractor is what the concrete integration can access
type Extractor interface {
	// Load a stream with shared config and state
	Load(config, state interface{}) error

	Schema() Schema

	// Batch emit records from a prepared http request
	// (probably) called multiple times
	// resp: (pre-allocated and reusable)
	// keys: (path to the data array)
	Batch(ctx context.Context, req *requests.Request, resp *requests.JSONResponse, keys ...string) error

	// State emit the state
	State(v interface{}) error
}

type Proto interface {
	// Open a new stream loader. Should emit or record the schema information
	Open(typ Schema) ExtendedStreamLoader

	// Spec defines the available streams
	Spec(ConnectorSpecification) error

	// Close closes the current session. Flushes pending data
	Close() error

	// ActiveStreams defines the active streams. 0 streams disable the filtering.
	ActiveStreams() Streams
}

type ExtendedStreamLoader interface {
	Extractor

	// Log something
	Log(v interface{}) error

	// Status report whether the credentials/config are correct, internally it is making one http request to check
	Status(v error) error
}

type RunnerFunc func(ctx context.Context, pw Extractor) error

func (r RunnerFunc) Run(ctx context.Context, pw Extractor) error {
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

func Open(r io.Reader, w io.Writer, cmd Command, protos Protos) (Proto, error) {
	var p fastjson.Parser
	i := &Protocol{states: map[string][]byte{}, _w: w, cmd: cmd}
	var buf []byte

	marshal := func(v *fastjson.Value) []byte {
		if v == nil {
			return nil
		}
		var out []byte
		buf = v.MarshalTo(buf[:0])
		return append(out, buf...)
	}

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		b := scanner.Bytes()
		if len(b) == 0 {
			continue
		}
		v, err := p.ParseBytes(b)
		if err != nil {
			return nil, err
		}
		switch t := msgType(v.GetStringBytes("type")); t {
		case "SETTINGS":
			b := marshal(v.Get("settings"))
			if err := json.NewDecoder(bytes.NewReader(b)).Decode(&i.settings); err != nil {
				return nil, err
			}
		case "CONFIG":
			i.config = marshal(v.Get("config"))
		case STATE:
			stream := string(v.GetStringBytes("stream"))
			i.states[stream] = marshal(v.Get("state"))
		default:
			return nil, fmt.Errorf("invalid type '%s'", t)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	useGlobalState := len(i.states[""]) > 0
	if useGlobalState {
		states := map[string]json.RawMessage{}

		if err := json.NewDecoder(bytes.NewReader(i.states[""])).Decode(&states); err != nil {
			panic(err)
		}
		delete(i.states, "")
		for k, v := range states {
			i.states[k] = v
		}
	}

	fn, ok := protos[i.settings.Format]
	if !ok {
		return nil, fmt.Errorf("not supported")
	}
	return fn(i), nil
}

func newWrap(typ msgType, stream string) *fastjson.Value {
	var a fastjson.Arena
	o := a.NewObject()
	o.Set("type", a.NewString(string(typ)))
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

type (
	runners   []runnerTyp
	runnerTyp struct {
		fn     Runner
		schema Schema
	}
)

type runner struct {
	config  interface{}
	runners runners
	protos  Protos
}

func (r *runner) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	p := strings.Split(request.URL.Path, "/")
	last := p[len(p)-1]

	if err := r.Handle(request.Context(), Command(last), writer, request.Body, r.protos); err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	}
}

type Command string

const (
	cmdSpec     Command = "spec"
	cmdCheck    Command = "check"
	cmdDiscover Command = "discover"
	cmdRead     Command = "read"
)

type msgType string

const (
	RECORD            msgType = "RECORD"
	STATE             msgType = "STATE"
	LOG               msgType = "LOG"
	CONNECTION_STATUS msgType = "CONNECTION_STATUS"
	CATALOG           msgType = "CATALOG"
	SPEC              msgType = "SPEC"
)

type (
	ProtoFn func(protocol *Protocol) Proto
	Protos  map[string]ProtoFn
)

func (r *runner) Handle(ctx context.Context, cmd Command, writer io.Writer, rd io.Reader, protos Protos) error {
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

func (r *runner) handle(ctx context.Context, proto Proto, cmd Command) error {
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

func New(config interface{}) *runner {
	return &runner{config: config}
}

type Loader interface {
	http.Handler
	Validate() error
	Protos(Protos)
	Handle(ctx context.Context, cmd Command, writer io.Writer, rd io.Reader, protos Protos) error
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

func (m *validatorLoader) Batch(ctx context.Context, req *requests.Request, resp *requests.JSONResponse, keys ...string) error {
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
	streams := proto.ActiveStreams()
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
			s := debug.Stack()
			log.Println(string(s))
			err = panicErr(s)
		}
		if err != nil {
			err = pw.Log(err)
		}
	}()

	if sync {
		return runner.fn.Run(ctx, pw)
	}
	return nil
}
