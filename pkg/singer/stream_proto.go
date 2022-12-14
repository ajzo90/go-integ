package singer

import (
	"context"

	"github.com/ajzo90/go-integ"
	"github.com/ajzo90/go-requests"
	"github.com/valyala/fastjson"
)

func (m *streamProto) Load(config, state interface{}) error {
	return m.i.Load(m.schema.Name, config, state)
}

type streamProto struct {
	rec        *fastjson.Value
	regStateFn func(v interface{})
	recBuf     []byte
	i          *proto
	schema     integ.Schema
}

func (m *streamProto) EmitBatch(ctx context.Context, req *requests.Request, resp *requests.JSONResponse, path ...string) error {
	err := req.Extended().ExecJSONPreAlloc(resp, ctx)
	if err != nil {
		return err
	}
	m.recBuf = m.recBuf[:0]

	record := m.rec.GetObject()

	for _, v := range resp.GetArray(path...) {
		record.Set("record", v)
		m.recBuf = append(m.rec.MarshalTo(m.recBuf), '\n')
	}
	return m.i.Write(m.recBuf)
}

func (m *streamProto) EmitState(v interface{}) error {
	m.regStateFn(v)
	return nil
}

func (m *streamProto) EmitLog(v interface{}) error {
	return m.i.emit(integ.LOG, logErr(v))
}

func logErr(v interface{}) interface{} {
	if err, ok := v.(error); ok {
		return err.Error()
	}
	return v
}
