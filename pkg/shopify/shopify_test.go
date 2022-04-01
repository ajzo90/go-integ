package shopify

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/ajzo90/go-integ/pkg/integ"
	"github.com/ajzo90/go-requests"
	"github.com/matryer/is"
	"github.com/valyala/fastjson"
	"io"
	"log"
	"strings"
	"testing"
)

type Settings struct {
	Format string
}
type integration struct {
	settings Settings
	config   []byte
	states   map[string][]byte
}

type mockStream struct {
	i      *integration
	stream string
}

func Open(r io.Reader) (integ.Proto, error) {

	var p fastjson.Parser
	var i = &integration{states: map[string][]byte{}}
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
		switch string(v.GetStringBytes("type")) {
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
			panic(1)
		}
	}

	return i, scanner.Err()
}

func (m *integration) Load(stream string, config, state interface{}) error {
	if len(m.config) > 0 {
		if err := json.NewDecoder(bytes.NewReader(m.config)).Decode(config); err != nil {
			return err
		}
	} else if config != nil {
		return fmt.Errorf("expected config")
	}

	if v := m.states[stream]; len(v) == 0 {
		return nil
	} else if err := json.NewDecoder(bytes.NewReader(v)).Decode(state); err != nil {
		return err
	}
	log.Println("loaded state", stream, state, config)
	return nil

}

func (m *integration) Open(name string) integ.Loader {
	switch m.settings.Format {
	case "mock":
		return &mockStream{stream: name, i: m}
	default:
		panic(1)
	}
}

func (m mockStream) Load(config, state interface{}) error {
	return m.i.Load(m.stream, config, state)
}

func (m mockStream) WriteBatch(ctx context.Context, q *requests.Request, keys ...string) (*requests.JSONResponse, error) {
	resp, err := q.ExecJSON(ctx)
	if err != nil {
		return nil, err
	}
	for _, v := range resp.GetArray(keys...) {
		log.Println("data", m.stream, v)
	}
	return resp, err
}

func (m mockStream) State(v interface{}) error {
	log.Println("new state", m.stream, v)
	return nil
}

func TestRun(t *testing.T) {
	is := is.New(t)
	p, err := Open(strings.NewReader(mockData))
	is.NoErr(err)

	var r = Runner("customers", user{})
	is.NoErr(r.Run(context.Background(), p.Open("users")))
}

/*
note:
HTTP request bodies are theoretically allowed for all methods except TRACE, however they are not commonly used except in PUT, POST and PATCH. Because of this, they may not be supported properly by some client frameworks, and you should not allow request bodies for GET, DELETE, TRACE, OPTIONS and HEAD methods.
*/
//var shopifyDoer = requests.NewRetryer(http.DefaultClient, requests.Logger(func(id int, err error, msg string) {
//	log.Println(id, err, msg)
//}))
