package airbyte

import (
	"github.com/ajzo90/go-integ"
	"github.com/valyala/fastjson"
)

func (m *streamProto) Load(config, state interface{}) error {
	return m.p.Load(m.schema.Name, config, state)
}

type streamProto struct {
	rec        *fastjson.Value
	regStateFn func(v interface{})
	recBuf     []byte
	p          *proto
	schema     integ.Schema
}

func (m *streamProto) EmitValues(arr []*fastjson.Value) error {
	record := m.rec.GetObject("record")
	for _, v := range arr {
		record.Set("data", v)
		m.recBuf = append(m.rec.MarshalTo(m.recBuf), '\n')
	}
	return m.flush(false)
}

func (m *streamProto) flush(last bool) error {
	if last || len(m.recBuf) > 4096 {
		err := m.p.Write(m.recBuf)
		m.recBuf = m.recBuf[:0]
		return err
	}
	return nil
}

func (m *streamProto) Flush() error {
	return m.flush(true)
}

func (m *streamProto) EmitState(v interface{}) error {
	m.regStateFn(v)
	return nil
}

func (m *streamProto) EmitLog(v interface{}) error {
	return m.p.emit(integ.LOG, logErr(v))
}

func logErr(v interface{}) interface{} {
	if err, ok := v.(error); ok {
		return err.Error()
	}
	return v
}
