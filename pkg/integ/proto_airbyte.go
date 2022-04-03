package integ

import (
	"context"
	"github.com/ajzo90/go-requests"
	"github.com/valyala/fastjson"
)

var AirbyteProto ProtoFn = func(p *Protocol) Proto {
	return &airbyteProto{p}
}

type airbyteProto struct {
	*Protocol
}

type airbyteStream struct {
	jsonStream
	streams []interface{}
	state   map[string]interface{}
	rec     *fastjson.Value
}

func (m *airbyteProto) Open(schema Schema) ExtendedStreamLoader {
	return &airbyteStream{rec: newWrap("RECORD", schema.Name), jsonStream: jsonStream{i: m.Protocol, schema: schema}, state: map[string]interface{}{}}
}

// Close flushes remaining data (state, streams)
func (m *airbyteProto) Close() error {
	switch m.cmd {
	case cmdDiscover:
		// write streams
	case cmdRead:
		// write state
	}
	return nil
}

func (m *airbyteProto) Spec(v ConnectorSpecification) error {
	return m.encode(struct {
		Type string      `json:"type"`
		Spec interface{} `json:"spec"`
	}{
		Type: "SPEC",
		Spec: v,
	})
}

func (m *airbyteStream) WriteBatch(ctx context.Context, req *requests.Request, resp *requests.JSONResponse, keys ...string) error {
	err := req.Extended().ExecJSONPreAlloc(resp, ctx)
	if err != nil {
		return err
	}
	m.recBuf = m.recBuf[:0]
	for _, v := range resp.GetArray(keys...) {
		m.rec.Set("record", v)
		m.recBuf = append(m.rec.MarshalTo(m.recBuf), '\n')
	}
	return m.i.Write(m.recBuf)
}

func (m *airbyteStream) WriteSchema(v Schema) error {
	m.streams = append(m.streams, v)
	return nil
}

func (m *airbyteStream) State(v interface{}) error {
	m.state[m.schema.Name] = v
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
