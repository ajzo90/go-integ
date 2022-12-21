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
	"strings"

	"github.com/ajzo90/go-jsonschema-generator"
	"github.com/ajzo90/go-requests"
	"github.com/klauspost/compress/zstd"
	"github.com/valyala/fastjson"
)

type Loader interface {
	Handle(ctx context.Context, cmd Command, w io.Writer, r io.Reader, protos Protos) error
}

type HttpRunnerFunc func(ctx HttpContext) error

func (r HttpRunnerFunc) Run(ctx HttpContext) error {
	return r(ctx)
}

type ManualRunnerFunc func(ctx ManualContext) error

func (r ManualRunnerFunc) Run(ctx ManualContext) error {
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
		case SETTINGS:
			b := marshal(v.Get("settings"))
			if err := json.NewDecoder(bytes.NewReader(b)).Decode(&i.settings); err != nil {
				return nil, err
			}
		case CONFIG:
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

type (
	runners   []runnerTyp
	runnerTyp struct {
		httpRunner HttpRunner
		fsRunner   FsRunner
		schema     Schema
	}
)

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
	SCHEMA            MsgType = "SCHEMA"

	CONFIG   MsgType = "CONFIG"
	SETTINGS MsgType = "SETTINGS"
)

type (
	ProtoFn func(protocol *Protocol) Proto
	Protos  map[string]ProtoFn
	Loaders map[string]Loader
)

func Handler(loaders Loaders, protos Protos) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var p = r.URL.Path
		if strings.HasPrefix(p, "/discover") {
			var o []string
			for k := range loaders {
				o = append(o, k)
			}
			if err := json.NewEncoder(w).Encode(o); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}

		for name, loader := range loaders {
			if len(p) >= 2+len(name) && p[1+len(name)] == '/' && p[1:][:len(name)] == name {
				serveLoader(loader, protos).ServeHTTP(w, r)
				return
			}
		}

		http.NotFound(w, r)
	}
}

type nopCloser struct {
	w io.Writer
}

func (n2 nopCloser) Close() error {
	if c, ok := n2.w.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

func (n2 nopCloser) Write(p []byte) (n int, err error) {
	return n2.w.Write(p)
}

func serveLoader(loader Loader, protos Protos) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		p := strings.Split(request.URL.Path, "/")
		last := p[len(p)-1]

		var wc io.WriteCloser = nopCloser{w: writer}

		var useZstd = request.Header.Get("Accept-Zstd") != ""
		if useZstd {
			writer.Header().Add("X-Compression", "zstd")
			zW, err := zstd.NewWriter(wc)
			if err != nil {
				panic(err)
			}
			wc = zW
		}

		if err := loader.Handle(request.Context(), Command(last), wc, request.Body, protos); err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
		} else if err := wc.Close(); err != nil {
			panic(err)
		}
	}
}

type panicErr string

func (p panicErr) Error() string {
	return string(p)
}

type validatorLoader struct {
	httpRunContext
}

func (m *validatorLoader) EmitBatch(req *requests.Request, resp *requests.JSONResponse, path ...string) error {
	if err := req.Extended().ExecJSONPreAlloc(resp, m.ctx); err != nil {
		return err
	} else {
		return validatorOK
	}
}

var validatorOK = fmt.Errorf("validatorOK")

type baseRunContext struct {
	ctx    context.Context
	schema Schema
	StreamProto
}

func makeBaseRunCtx(ctx context.Context, schema Schema, sp StreamProto) baseRunContext {
	return baseRunContext{ctx: ctx, schema: schema, StreamProto: sp}
}

func (r *baseRunContext) Schema() Schema {
	return r.schema
}

func (r *baseRunContext) EmitValues(values []*fastjson.Value) error {
	if err := r.ctx.Err(); err != nil {
		return err
	}
	return r.StreamProto.EmitValues(values)
}

func (r *baseRunContext) EmitValue(value any) error {
	switch t := value.(type) {
	case *fastjson.Value:
		return r.EmitValues([]*fastjson.Value{t})
	case []*fastjson.Value:
		return r.EmitValues(t)
	default:
		b, err := json.Marshal([]any{value})
		if err != nil {
			return err
		}
		v, err := fastjson.ParseBytes(b)
		if err != nil {
			return err
		}
		return r.EmitValues(v.GetArray())
	}

}

type httpRunContext struct {
	baseRunContext
}

type ValueEmitter interface {
	EmitValues([]*fastjson.Value) error
}

func EmitBatch(ctx context.Context, emitter ValueEmitter, req *requests.Request, resp *requests.JSONResponse, keys ...string) error {
	err := req.Extended().ExecJSONPreAlloc(resp, ctx)
	if err != nil {
		return err
	}
	return emitter.EmitValues(resp.GetArray(keys...))
}

func (r *httpRunContext) EmitBatch(req *requests.Request, resp *requests.JSONResponse, keys ...string) error {
	return EmitBatch(r.ctx, r, req, resp, keys...)
}

func newHTTPRunCtx(ctx context.Context, schema Schema, sp StreamProto) *httpRunContext {
	return &httpRunContext{baseRunContext: makeBaseRunCtx(ctx, schema, sp)}
}

type ConnectorSpecification struct {
	DocumentationURL        string               `json:"documentationUrl,omitempty"`
	SupportsIncremental     bool                 `json:"supportsIncremental"`
	ConnectionSpecification *jsonschema.Document `json:"connectionSpecification"`
}

func run(ctx context.Context, proto Proto, runner runnerTyp, sync bool) (err error) {
	sp, err := proto.Open(runner.schema)
	if err != nil {
		return err
	} else if sp == nil {
		// skip stream
		return nil
	}

	defer func() {
		const useRecover = false
		if useRecover {
			if pErr := recover(); pErr != nil {
				s := debug.Stack()
				log.Println(string(s))
				err = panicErr(s)
			}
		}

		if err == nil {
			err = sp.Flush()
		}

		// check err again
		if err != nil {
			err = sp.EmitLog(err)
		}
	}()

	if sync {
		if runner.httpRunner != nil {
			runCtx := newHTTPRunCtx(ctx, runner.schema, sp)
			return runner.httpRunner.Run(runCtx)
		} else if runner.fsRunner != nil {
			return fmt.Errorf("fs runner not implemented")
		} else {
			return fmt.Errorf("runner not implemented")
		}
	}
	return nil
}

type BaseProtocol struct {
	recBuf []byte
	wr     io.Writer
}

func (m *BaseProtocol) flush(last bool) error {
	if last || len(m.recBuf) > 4096 {
		_, err := m.wr.Write(m.recBuf)
		m.recBuf = m.recBuf[:0]
		return err
	}
	return nil
}

func (m *BaseProtocol) Flush() error {
	return m.flush(true)
}
