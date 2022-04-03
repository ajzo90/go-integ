package integ

import (
	"context"
	"encoding/json"
	"github.com/ajzo90/go-jsonschema-generator"
	"github.com/ajzo90/go-requests"
	"github.com/valyala/fastjson"
)

func (m *mockIntegration) Open(name string, typ interface{}) ExtendedLoader {
	return &mockStream{stream: name, i: m, rec: newWrap("RECORD", name), typ: typ}
}

func (m *mockStream) Load(config, state interface{}) error {
	return m.i.Load(m.stream, config, state)
}

func (m *mockStream) WriteBatch(ctx context.Context, q *requests.Request, keys ...string) (*requests.JSONResponse, error) {
	resp, err := q.ExecJSON(ctx)
	if err != nil {
		return nil, err
	}
	m.recBuf = m.recBuf[:0]
	for _, v := range resp.GetArray(keys...) {
		m.rec.Set("record", v)
		m.recBuf = append(m.rec.MarshalTo(m.recBuf), '\n')
	}
	return resp, m.i.Write(m.recBuf)
}

func (m *mockStream) Fields() []string {
	return Keys(jsonschema.New(m.typ))
}
func (m *mockStream) Schema(v interface{}) error {
	return m.encode(struct {
		Type   string      `json:"type"`
		Stream string      `json:"stream"`
		Schema interface{} `json:"schema"`
	}{
		Type:   "SCHEMA",
		Stream: m.stream,
		Schema: jsonschema.New(v),
	})
}

func (m *mockStream) encode(v interface{}) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return m.i.Write(append(b, '\n'))
}

func (m *mockStream) State(v interface{}) error {
	return m.encode(struct {
		Type   string      `json:"type"`
		Stream string      `json:"stream"`
		State  interface{} `json:"state"`
	}{
		Type:   "STATE",
		Stream: m.stream,
		State:  v,
	})
}

type mockIntegration struct {
	*integration
}

type mockStream struct {
	i      *mockIntegration
	stream string
	typ    interface{}
	rec    *fastjson.Value
	recBuf []byte
}
