package integ

import (
	"context"

	"github.com/ajzo90/go-requests"
	"github.com/valyala/fastjson"
)

var AirbyteProto ProtoFn = func(p *Protocol) Proto {
	return &airbyteProto{Protocol: p, regState: map[string]interface{}{}}
}

type airbyteProto struct {
	*Protocol
	regState map[string]interface{}
	schemas  []Schema
}

type airbyteStream struct {
	jsonStream
	streams    []interface{}
	rec        *fastjson.Value
	regStateFn func(v interface{})
	recBuf     []byte
}

func (m *airbyteProto) Open(schema Schema) ExtendedStreamLoader {
	regStateFn := func(v interface{}) {
		m.regState[schema.Name] = v
	}
	m.schemas = append(m.schemas, schema)
	return &airbyteStream{regStateFn: regStateFn, rec: newWrap(RECORD, schema.Name), jsonStream: jsonStream{i: m.Protocol, schema: schema}}
}

// Close flushes remaining data (state, streams)
func (m *airbyteProto) Close() error {
	switch m.cmd {
	case cmdDiscover:
		return m.encode(struct {
			Type    msgType  `json:"type"`
			Catalog []Schema `json:"catalog"`
		}{
			Type:    CATALOG,
			Catalog: m.schemas,
		})
	case cmdRead:
		return m.encode(struct {
			Type  msgType     `json:"type"`
			State interface{} `json:"state"`
		}{
			Type:  STATE,
			State: m.regState,
		})
	}
	return nil
}

func (m *airbyteProto) Spec(v ConnectorSpecification) error {
	return m.encode(struct {
		Type msgType     `json:"type"`
		Spec interface{} `json:"spec"`
	}{
		Type: SPEC,
		Spec: v,
	})
}

func (m *airbyteStream) Batch(ctx context.Context, req *requests.Request, resp *requests.JSONResponse, keys ...string) error {
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
	m.regStateFn(v)
	return nil
}

type checkStatus string

const (
	SUCCEEDED checkStatus = "SUCCEEDED"
	FAILED    checkStatus = "FAILED"
)

func (m *airbyteStream) Status(err error) error {
	type Status struct {
		Status checkStatus `json:"status"`
		Reason string      `json:"reason,omitempty"`
	}
	var s Status
	s.Status = SUCCEEDED
	if err != nil {
		s.Status = FAILED
		s.Reason = err.Error()
	}
	return m.i.encode(struct {
		Type             msgType `json:"type"`
		ConnectionStatus Status  `json:"connectionStatus"`
	}{
		Type:             CONNECTION_STATUS,
		ConnectionStatus: s,
	})
}
