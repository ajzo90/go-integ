package integ

type jsonStream struct {
	i      *Protocol
	schema Schema
}

func (m *jsonStream) Load(config, state interface{}) error {
	return m.i.Load(m.schema.Name, config, state)
}

func (m *jsonStream) EmitState(v interface{}) error {
	return m.i.encode(struct {
		Type   msgType     `json:"type"`
		Stream string      `json:"stream"`
		State  interface{} `json:"state"`
	}{
		Type:   STATE,
		Stream: m.schema.Name,
		State:  v,
	})
}

//func (m *jsonStream) WriteSchema(v Schema) error {
//	return m.i.encode(struct {
//		Type   string      `json:"type"`
//		Stream string      `json:"stream"`
//		Schema interface{} `json:"schema"`
//	}{
//		Type:   "SCHEMA",
//		Stream: m.schema.Name,
//		Schema: jsonschema.New(v),
//	})
//}

func (m *jsonStream) EmitLog(v interface{}) error {
	return m.i.encode(struct {
		Type   msgType     `json:"type"`
		Stream string      `json:"stream"`
		Log    interface{} `json:"log"`
	}{
		Type:   LOG,
		Stream: m.schema.Name,
		Log:    logErr(v),
	})
}

func logErr(v interface{}) interface{} {
	if err, ok := v.(error); ok {
		return err.Error()
	}
	return v
}
