package go_integ

import "github.com/ajzo90/go-jsonschema-generator"

type Schema struct {
	Incremental       bool
	PrimaryKey        []FieldDef
	OrderByKey        []FieldDef
	IterateByKey      []FieldDef
	CustomPrimaryKey  bool
	CustomOrderByKey  bool
	CustomerIterateBy bool
	Name              string
	GoType            interface{}
	JsonSchema        *jsonschema.Document
	Namespace         string
}

func (s Schema) Validate() error {
	return nil
}

type SchemaBuilder struct {
	Schema
}

func NonIncremental(name string, typ interface{}) SchemaBuilder {
	return SchemaBuilder{Schema: Schema{Name: name, GoType: typ, JsonSchema: jsonschema.New(typ)}}
}

func Incremental(name string, typ interface{}) SchemaBuilder {
	v := NonIncremental(name, typ)
	v.Incremental = true
	return v
}

func (s SchemaBuilder) Namespace(namespace string) SchemaBuilder {
	s.Schema.Namespace = namespace
	return s
}

func (s SchemaBuilder) Primary(keys ...FieldDef) SchemaBuilder {
	s.PrimaryKey = keys
	return s
}

func (s SchemaBuilder) OrderBy(keys ...FieldDef) SchemaBuilder {
	s.OrderByKey = keys
	return s
}

func (s SchemaBuilder) IterateBy(keys ...FieldDef) SchemaBuilder {
	s.IterateByKey = keys
	return s
}

func (s SchemaBuilder) CustomPrimary() SchemaBuilder {
	s.CustomPrimaryKey = true
	return s
}

func (s SchemaBuilder) CustomOrderBy() SchemaBuilder {
	s.CustomOrderByKey = true
	return s
}

func (s SchemaBuilder) CustomIterateBy() SchemaBuilder {
	s.CustomerIterateBy = true
	return s
}

func (s Schema) FieldKeys() []string {
	return Keys(jsonschema.New(s.GoType))
}

// SupportedSyncModes      []SyncMode `json:"supported_sync_modes,omitempty"`
// SourceDefinedCursor     bool       `json:"source_defined_cursor,omitempty"`
// DefaultCursorField      []string   `json:"default_cursor_field,omitempty"`
// SourceDefinedPK [][]string `json:"source_defined_primary_key,omitempty"`
// Namespace               string     `json:"namespace"`

type FieldDef struct {
	Path          []string
	FieldEncoding string
	SortOrder     string
}

func Field(path ...string) FieldDef {
	return FieldDef{Path: path}
}

func (f FieldDef) Encoding(v string) FieldDef {
	f.FieldEncoding = v
	return f
}

func (f FieldDef) Asc() FieldDef {
	f.SortOrder = "ASC"
	return f
}

func (f FieldDef) Desc() FieldDef {
	f.SortOrder = "DESC"
	return f
}
