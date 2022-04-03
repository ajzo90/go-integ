package integ

import (
	"context"
	"github.com/ajzo90/go-requests"
	"github.com/valyala/fastjson"
)

var _ Proto = &airbyteProto{}
var _ ExtendedStreamLoader = &airbyteStream{}

type airbyteProto struct {
	*integration
}

func (m *airbyteProto) Open(name string, typ interface{}) ExtendedStreamLoader {
	return &airbyteStream{rec: newWrap("RECORD", name), jsonStream: jsonStream{i: m.integration, typ: typ, stream: name}}
}

// Close flushes remaining data (state, streams)
func (m *airbyteProto) Close() error {
	return nil
}

func (m *airbyteProto) Spec(v interface{}) error {
	return m.encode(struct {
		Type string      `json:"type"`
		Spec interface{} `json:"spec"`
	}{
		Type: "SPEC",
		Spec: v,
	})
}

type airbyteStream struct {
	jsonStream
	streams []interface{}
	state   map[string]interface{}
	rec     *fastjson.Value
}

func (m *airbyteStream) WriteBatch(ctx context.Context, q *requests.Request, keys ...string) (*requests.JSONResponse, error) {
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

func (m *airbyteStream) Schema(v interface{}) error {
	m.streams = append(m.streams, v)
	return nil
}

func (m *airbyteStream) State(v interface{}) error {
	m.state[m.stream] = v
	return nil
}

func (m *airbyteStream) Status(err error) error {
	type Status struct {
		Status string `json:"status"`
	}
	var s Status
	s.Status = "SUCCEEDED"
	if err != nil {
		s.Status = "FAILED"
	}

	return m.i.encode(struct {
		Type             string `json:"type"`
		ConnectionStatus Status `json:"connectionStatus"`
	}{
		Type:             "CONNECTION_STATUS",
		ConnectionStatus: s,
	})
}
