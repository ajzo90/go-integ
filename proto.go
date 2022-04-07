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

type HttpRunner interface {
	// Run runs the sync job.
	Run(ctx HttpContext) error
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
}

type HttpContext interface {
	GeneralContext

	// EmitBatch executes the provided request, locate the data array and emit the records
	// (likely) called multiple times in the same run
	// resp: (pre-allocated and reusable)
	// path: (path to the data array)
	EmitBatch(req *requests.Request, resp *requests.JSONResponse, path ...string) error
}

type DbContext interface {
	GeneralContext
}

type FsContext interface {
	GeneralContext
}

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

type RunnerFunc func(ctx HttpContext) error

func (r RunnerFunc) Run(ctx HttpContext) error {
	return r(ctx)
}

type Streams []Schema

type Settings struct {
	Format  string
	Streams Streams
}

func Open(r io.Reader, w io.Writer, cmd Command, protos Protos) (Proto, error) {
	var p fastjson.Parser
	i := &Protocol{states: map[string][]byte{}, _w: w, Cmd: cmd}
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
		switch t := MsgType(v.GetStringBytes("type")); t {
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
		case CATALOG:

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
		httpRunner HttpRunner
		fsRunner   FsRunner
		schema     Schema
	}
)

type sourceDef struct {
	config     interface{}
	runners    runners
	protos     Protos
	httpRunner HttpRunner
}

type Command string

const (
	CmdSpec     Command = "spec"
	CmdCheck    Command = "check"
	CmdDiscover Command = "discover"
	CmdRead     Command = "read"
)

type MsgType string

const (
	RECORD            MsgType = "RECORD"
	STATE             MsgType = "STATE"
	LOG               MsgType = "LOG"
	CONNECTION_STATUS MsgType = "CONNECTION_STATUS"
	CATALOG           MsgType = "CATALOG"
	SPEC              MsgType = "SPEC"
)

type (
	ProtoFn func(protocol *Protocol) Proto
	Protos  map[string]ProtoFn
)

func (r *sourceDef) Handle(ctx context.Context, cmd Command, writer io.Writer, rd io.Reader, protos Protos) error {
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

func (r *sourceDef) Protos(protos Protos) {
	r.protos = protos
}

func (r *sourceDef) handle(ctx context.Context, proto Proto, cmd Command) error {
	switch cmd {
	case CmdSpec:
		return r.Spec(ctx, proto)
	case CmdCheck:
		return r.Check(ctx, proto)
	case CmdDiscover:
		return r.Run(ctx, proto, false)
	case CmdRead:
		return r.Run(ctx, proto, true)
	default:
		return fmt.Errorf("invalid path")
	}
}

func NewSource(config interface{}) *sourceDef {
	return &sourceDef{config: config}
}

func (r *sourceDef) Documentation(links ...string) *sourceDef {
	return r
}

func (r *sourceDef) Notes(links ...string) *sourceDef {
	return r
}

func Server(loader Loader, protos Protos) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		p := strings.Split(request.URL.Path, "/")
		last := p[len(p)-1]

		if err := loader.Handle(request.Context(), Command(last), writer, request.Body, protos); err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
		}
	})
}

type Loader interface {
	Handle(ctx context.Context, cmd Command, w io.Writer, r io.Reader, protos Protos) error
}

func (r *sourceDef) HttpRunner(runner HttpRunner) *sourceDef {
	r.httpRunner = runner
	return r
}

func (r *sourceDef) GeneralStream(schema SchemaBuilder, runner ...GeneralRunner) *sourceDef {
	return r
}

func (r *sourceDef) FsStream(schema SchemaBuilder, runner ...FsRunner) *sourceDef {
	return r
}

func (r *sourceDef) DbStream(schema SchemaBuilder, runner ...DbRunner) *sourceDef {
	return r
}

func (r *sourceDef) HttpStream(schema SchemaBuilder, runner ...HttpRunner) *sourceDef {
	var fn HttpRunner
	if len(runner) == 1 {
		fn = runner[0]
	}
	r.runners = append(r.runners, runnerTyp{schema: schema.Schema, httpRunner: fn})
	return r
}

type panicErr string

func (p panicErr) Error() string {
	return string(p)
}

type validatorLoader struct {
	runContext
}

func (m *validatorLoader) EmitBatch(req *requests.Request, resp *requests.JSONResponse, path ...string) error {
	if err := req.Extended().ExecJSONPreAlloc(resp, m.ctx); err != nil {
		return err
	} else {
		return validatorOK
	}
}

var validatorOK = fmt.Errorf("validatorOK")

func (r *sourceDef) Validate() error {
	for _, runner := range r.runners {
		if err := runner.schema.Validate(); err != nil {
			return err
		}
	}
	return nil
}

type runContext struct {
	ctx    context.Context
	schema Schema
	StreamProto
}

func (r *runContext) Schema() Schema {
	return r.schema
}

func (r *runContext) EmitBatch(req *requests.Request, resp *requests.JSONResponse, path ...string) error {
	return r.StreamProto.EmitBatch(r.ctx, req, resp, path...)
}

func newRunCtx(ctx context.Context, schema Schema, proto Proto) *runContext {
	log.Println("SCHEMA", schema)
	return &runContext{ctx: ctx, schema: schema, StreamProto: proto.Open(schema)}
}

func (r *sourceDef) Check(ctx context.Context, proto Proto) error {
	for _, runner := range r.runners {
		runCtx := newRunCtx(ctx, runner.schema, proto)
		if err := runner.httpRunner.Run(&validatorLoader{runContext: *runCtx}); err == validatorOK {
			return proto.EmitStatus(nil)
		} else if err != nil {
			return proto.EmitStatus(fmt.Errorf("validation failed: %s", err.Error()))
		}
	}
	return fmt.Errorf("validation failed: unexpected error")
}

func (r *sourceDef) Discover(ctx context.Context, proto Proto) error {
	return r.Run(ctx, proto, false)
}

type ConnectorSpecification struct {
	DocumentationURL        string               `json:"documentationUrl,omitempty"`
	SupportsIncremental     bool                 `json:"supportsIncremental"`
	ConnectionSpecification *jsonschema.Document `json:"connectionSpecification"`
}

func (r *sourceDef) Spec(ctx context.Context, proto Proto) error {
	return proto.EmitSpec(ConnectorSpecification{
		DocumentationURL:        "127.0.0.1/docs",
		SupportsIncremental:     true, // why is this important to share?
		ConnectionSpecification: jsonschema.New(r.config),
	})
}

func (r *sourceDef) Read(ctx context.Context, proto Proto) error {
	return r.Run(ctx, proto, true)
}

func (r *sourceDef) Run(ctx context.Context, proto Proto, sync bool) error {
	wg, ctx := errgroup.WithContext(ctx)
	for _, runner := range r.runners {
		runner := runner // copy
		wg.Go(func() (err error) {
			return run(ctx, proto, runner, sync)
		})
	}
	return wg.Wait()
}

const useRecover = false

func run(ctx context.Context, proto Proto, runner runnerTyp, sync bool) (err error) {
	pw := newRunCtx(ctx, runner.schema, proto)
	if pw.StreamProto == nil {
		// skip stream
		return nil
	}

	defer func() {
		if useRecover {
			if pErr := recover(); pErr != nil {
				s := debug.Stack()
				log.Println(string(s))
				err = panicErr(s)
			}
		}

		if err != nil {
			err = pw.EmitLog(err)
		}
	}()

	if sync {
		if runner.httpRunner != nil {
			return runner.httpRunner.Run(pw)
		} else if runner.fsRunner != nil {
			return runner.fsRunner.Run(pw)
		} else {
			return fmt.Errorf("runner not implemented")
		}
	}
	return nil
}
