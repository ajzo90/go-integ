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
	"github.com/valyala/fastjson"
)

type Loader interface {
	Handle(ctx context.Context, cmd Command, w io.Writer, r io.Reader, protos Protos) error
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
)

type (
	ProtoFn func(protocol *Protocol) Proto
	Protos  map[string]ProtoFn
)

func Server(loader Loader, protos Protos) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		p := strings.Split(request.URL.Path, "/")
		last := p[len(p)-1]

		if err := loader.Handle(request.Context(), Command(last), writer, request.Body, protos); err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
		}
	})
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
	return &runContext{ctx: ctx, schema: schema, StreamProto: proto.Open(schema)}
}

type ConnectorSpecification struct {
	DocumentationURL        string               `json:"documentationUrl,omitempty"`
	SupportsIncremental     bool                 `json:"supportsIncremental"`
	ConnectionSpecification *jsonschema.Document `json:"connectionSpecification"`
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
