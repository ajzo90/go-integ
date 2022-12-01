package airbyte

import (
	"strings"
	"time"

	"github.com/ajzo90/go-integ"
	"github.com/valyala/fastjson"
)

var Proto integ.ProtoFn = func(p *integ.Protocol) integ.Proto {
	return &proto{Protocol: p, regState: map[string]interface{}{}}
}

type proto struct {
	*integ.Protocol
	regState map[string]interface{}
	schemas  []integ.Schema
}

func newRecord(stream string) *fastjson.Value {
	var a fastjson.Arena
	o := a.NewObject()
	o.Set("type", a.NewString(string(integ.RECORD)))

	record := a.NewObject()
	record.Set("stream", a.NewString(stream))
	record.Set("emitted_at", a.NewNumberInt(int(time.Now().UnixMilli())))
	o.Set("record", record)

	return o
}

func (m *proto) Open(schema integ.Schema) integ.StreamProto {
	regStateFn := func(v interface{}) {
		m.regState[schema.Name] = v
	}
	m.schemas = append(m.schemas, schema)
	return &streamProto{i: m, regStateFn: regStateFn, rec: newRecord(schema.Name), schema: schema}
}

// Close flushes remaining data (state, streams)
func (m *proto) Close() error {
	switch m.Cmd {
	case integ.CmdDiscover:
		return m.emit(integ.CATALOG, m.schemas)
	case integ.CmdRead:
		return m.emit(integ.STATE, m.regState)
	}
	return nil
}

func (m *proto) emit(typ integ.MsgType, v interface{}) error {
	return m.Encode(map[string]interface{}{"type": typ, strings.ToLower(string(typ)): v})
}

type checkStatus string

const (
	SUCCEEDED checkStatus = "SUCCEEDED"
	FAILED    checkStatus = "FAILED"
)

func (m *proto) EmitSpec(v integ.ConnectorSpecification) error {
	return m.emit(integ.SPEC, v)
}

func (m *proto) EmitStatus(err error) error {
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
	return m.emit(integ.CONNECTION_STATUS, s)
}
