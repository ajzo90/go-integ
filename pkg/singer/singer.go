package singer

import (
	"github.com/ajzo90/go-integ"
	"github.com/ajzo90/go-jsonschema-generator"
	"github.com/valyala/fastjson"
	"strings"
	"time"
)

var Proto integ.ProtoFn = func(p *integ.Protocol) integ.Proto {
	return &singer{Protocol: p}
}

type singer struct {
	*integ.Protocol
}

func newRecordSerializer(stream string) func(buf []byte, v *fastjson.Value) []byte {
	var staticArena, localArena fastjson.Arena

	o := staticArena.NewObject()
	o.Set("type", staticArena.NewString(string(integ.RECORD)))
	o.Set("stream", staticArena.NewString(stream))

	return func(buf []byte, v *fastjson.Value) []byte {
		localArena.Reset()
		o.Set("time_extracted", localArena.NewNumberInt(int(time.Now().Unix())))
		o.Set("record", v)
		return append(o.MarshalTo(buf), '\n')
	}
}

type schemaMsg struct {
	Type              string               `json:"type"`
	Schema            *jsonschema.Document `json:"schema"`
	Stream            string               `json:"stream"`
	KeyProperties     []string             `json:"key_properties"`
	OrderByProperties []string             `json:"order_by_properties"`
}

func (m *singer) Open(schema integ.Schema) (integ.StreamProto, error) {
	// emit stream info. number of records?
	var pks []string
	for _, v := range schema.PrimaryKey {
		pks = append(pks, v.Path[0])
	}

	var extractKey = func(fields []integ.FieldDef) []string {
		if len(fields) == 0 {
			return nil
		}
		return fields[0].Path[:1]
	}

	err := m.Encode(schemaMsg{
		Type:              string(integ.SCHEMA),
		Stream:            schema.Name,
		KeyProperties:     extractKey(schema.PrimaryKey),
		OrderByProperties: extractKey(schema.OrderByKey),
		Schema:            schema.JsonSchema,
	})

	return &singerStream{p: m, serialize: newRecordSerializer(schema.Name), schema: schema}, err
}

// Close flushes remaining data (state, streams)
func (m *singer) Close() error {
	// summary?
	return nil
}

func (m *singer) emit(typ integ.MsgType, v interface{}) error {
	mapp := map[string]interface{}{"type": typ, strings.ToLower(string(typ)): v}
	return m.Encode(mapp)
}

type checkStatus string

const (
	SUCCEEDED checkStatus = "SUCCEEDED"
	FAILED    checkStatus = "FAILED"
)

func (m *singer) EmitSpec(v integ.ConnectorSpecification) error {
	return m.emit(integ.SPEC, v)
}

func (m *singer) EmitStatus(err error) error {
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
