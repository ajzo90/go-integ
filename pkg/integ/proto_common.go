package integ

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/ajzo90/go-jsonschema-generator"
	"github.com/ajzo90/go-requests"
	"io"
	"sync"
)

var _ ExtendedStreamLoader = &jsonStream{}

func (m *jsonStream) WriteBatch(ctx context.Context, q *requests.Request, keys ...string) (*requests.JSONResponse, error) {
	panic("implement me")
}

func (m *jsonStream) Status(v error) error {
	panic("implement me")
}

type jsonStream struct {
	i      *integration
	typ    interface{}
	stream string
	recBuf []byte
}

func (m *jsonStream) Load(config, state interface{}) error {
	return m.i.Load(m.stream, config, state)
}

func (m *jsonStream) Fields() []string {
	return Keys(jsonschema.New(m.typ))
}

func (m *jsonStream) State(v interface{}) error {
	return m.i.encode(struct {
		Type   string      `json:"type"`
		Stream string      `json:"stream"`
		State  interface{} `json:"state"`
	}{
		Type:   "STATE",
		Stream: m.stream,
		State:  v,
	})
}

func (m *jsonStream) Schema(v interface{}) error {
	return m.i.encode(struct {
		Type   string      `json:"type"`
		Stream string      `json:"stream"`
		Schema interface{} `json:"schema"`
	}{
		Type:   "SCHEMA",
		Stream: m.stream,
		Schema: jsonschema.New(v),
	})
}

func (m *jsonStream) Log(v interface{}) error {
	return m.i.encode(struct {
		Type   string      `json:"type"`
		Stream string      `json:"stream"`
		Log    interface{} `json:"log"`
	}{
		Type:   "LOG",
		Stream: m.stream,
		Log:    jsonschema.New(v),
	})
}

type integration struct {
	settings Settings
	config   []byte
	states   map[string][]byte
	_w       io.Writer
	wMtx     sync.Mutex
}

func (i *integration) Streams() Streams {
	return i.settings.Streams
}

func (i *integration) encode(v interface{}) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return i.Write(append(b, '\n'))
}

func (i *integration) Write(b []byte) error {
	i.wMtx.Lock()
	defer i.wMtx.Unlock()
	_, err := i._w.Write(b)
	return err
}

func (i *integration) Load(stream string, config, state interface{}) error {
	if len(i.config) > 0 {
		if err := json.NewDecoder(bytes.NewReader(i.config)).Decode(config); err != nil {
			return err
		}
	} else if config != nil {
		return fmt.Errorf("expected config")
	}

	if v := i.states[stream]; len(v) == 0 {
		return nil
	} else if err := json.NewDecoder(bytes.NewReader(v)).Decode(state); err != nil {
		return err
	}
	return nil
}
