package singer

import (
	"github.com/ajzo90/go-integ"
	"github.com/valyala/fastjson"
	"strings"
)

var Proto integ.ProtoFn = func(p *integ.Protocol) integ.Proto {
	return &proto{Protocol: p, regState: map[string]interface{}{}}
}

type proto struct {
	*integ.Protocol
	regState map[string]interface{}
}

func newRecord(stream string) *fastjson.Value {
	var a fastjson.Arena
	o := a.NewObject()
	o.Set("type", a.NewString(string(integ.RECORD)))
	o.Set("stream", a.NewString(stream))
	return o
}

func (m *proto) Open(schema integ.Schema) integ.StreamProto {
	regStateFn := func(v interface{}) {
		m.regState[schema.Name] = v
	}
	var pks []string
	for _, v := range schema.PrimaryKey {
		pks = append(pks, v.Path[0])
	}

	m.emit(integ.SCHEMA, schema.JsonSchema, func(m map[string]any) {
		m["stream"] = schema.Name
		m["key_properties"] = schema.PrimaryKey[0].Path[:1]
	}) // emit schema?
	return &streamProto{i: m, regStateFn: regStateFn, rec: newRecord(schema.Name), schema: schema}
}

// Close flushes remaining data (state, streams)
func (m *proto) Close() error {
	switch m.Cmd {
	case integ.CmdRead:
		return m.emit(integ.STATE, m.regState)
	}
	return nil
}

func (m *proto) emit(typ integ.MsgType, v interface{}, fns ...func(map[string]any)) error {
	mapp := map[string]interface{}{"type": typ, strings.ToLower(string(typ)): v}
	for _, fn := range fns {
		fn(mapp)
	}

	return m.Encode(mapp)
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
