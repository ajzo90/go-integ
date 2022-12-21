package singer

import (
	"time"

	"github.com/ajzo90/go-integ"
	"github.com/valyala/fastjson"
)

func (m *singerStream) Load(config, state interface{}) error {
	return m.p.Load(m.schema.Name, config, state)
}

type singerStream struct {
	serialize func([]byte, *fastjson.Value) []byte
	recBuf    []byte
	p         *singer
	schema    integ.Schema
}

func (m *singerStream) EmitValues(arr []*fastjson.Value) error {
	for _, v := range arr {
		m.recBuf = m.serialize(m.recBuf, v)
	}
	return m.flush(false)
}

func (m *singerStream) flush(forcedFlush bool) error {
	if forcedFlush || len(m.recBuf) > 4096 {
		err := m.p.Write(m.recBuf)
		m.recBuf = m.recBuf[:0]
		return err
	}
	return nil
}

func (m *singerStream) Flush() error {
	return m.flush(true)
}

func (m *singerStream) EmitState(v interface{}) error {

	type singerState struct {
		Type   string `json:"type"`
		Stream string `json:"stream"`
		State  any    `json:"state"`
	}

	if err := m.flush(true); err != nil {
		return err
	}

	return m.p.Encode(singerState{
		Type:   string(integ.STATE),
		Stream: m.schema.Name,
		State:  v,
	})
}

func (m *singerStream) EmitLog(v interface{}) error {

	type singerLog struct {
		Type      string `json:"type"`
		Timestamp int64  `json:"timestamp"`
		Stream    string `json:"stream"`
		Log       any    `json:"log"`
	}

	if err := m.flush(true); err != nil {
		return err
	}

	return m.p.Encode(singerLog{
		Type:      string(integ.LOG),
		Stream:    m.schema.Name,
		Log:       logErr(v),
		Timestamp: time.Now().Unix(),
	})
}

func logErr(v any) any {
	if err, ok := v.(error); ok {
		return err.Error()
	}
	return v
}
