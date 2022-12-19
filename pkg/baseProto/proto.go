package baseProto

import (
	"github.com/ajzo90/go-integ"
	"github.com/valyala/fastjson"
	"io"
)

type BaseStreamProto struct {
	record *fastjson.Value
	recBuf []byte
	Schema integ.Schema
	w      io.Writer
}

func (m *BaseStreamProto) flush(last bool) error {
	if last || len(m.recBuf) > 4096 {
		_, err := m.w.Write(m.recBuf)
		m.recBuf = m.recBuf[:0]
		return err
	}
	return nil
}

func (m *BaseStreamProto) Flush() error {
	return m.flush(true)
}
