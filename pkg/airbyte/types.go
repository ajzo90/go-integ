package airbyte

import "github.com/ajzo90/go-jsonschema-generator"

// SyncMode defines the modes that your source is able to sync in
type SyncMode string

const (
	// SyncModeFullRefresh means the data will be wiped and fully synced on each run
	SyncModeFullRefresh SyncMode = "full_refresh"
	// SyncModeIncremental is used for incremental syncs
	SyncModeIncremental SyncMode = "incremental"
)

// DestinationSyncMode represents how the destination should interpret your data
type DestinationSyncMode string

var (
	// DestinationSyncModeAppend is used for the destination to know it needs to append data
	DestinationSyncModeAppend DestinationSyncMode = "append"
	// DestinationSyncModeOverwrite is used to indicate the destination should overwrite data
	DestinationSyncModeOverwrite DestinationSyncMode = "overwrite"
)

// Catalog defines the complete available schema you can sync with a source
// This should not be mistaken with ConfiguredCatalog which is the "selected" schema you want to sync
type Catalog struct {
	Streams []Stream `json:"streams"`
}

// Stream defines a single "schema" you'd like to sync - think of this as a table, collection, topic, etc. In airbyte terminology these are "streams"
type Stream struct {
	Name                    string               `json:"name"`
	JSONSchema              *jsonschema.Document `json:"json_schema"`
	SupportedSyncModes      []SyncMode           `json:"supported_sync_modes,omitempty"`
	SourceDefinedCursor     bool                 `json:"source_defined_cursor,omitempty"`
	DefaultCursorField      []string             `json:"default_cursor_field,omitempty"`
	SourceDefinedPrimaryKey [][]string           `json:"source_defined_primary_key,omitempty"`
	Namespace               string               `json:"namespace"`
}

// ConfiguredCatalog is the "selected" schema you want to sync
// This should not be mistaken with Catalog which represents the complete available schema to sync
type ConfiguredCatalog struct {
	Streams []ConfiguredStream `json:"streams"`
}

// ConfiguredStream defines a single selected stream to sync
type ConfiguredStream struct {
	Stream              Stream              `json:"stream"`
	SyncMode            SyncMode            `json:"sync_mode"`
	CursorField         []string            `json:"cursor_field"`
	DestinationSyncMode DestinationSyncMode `json:"destination_sync_mode"`
	PrimaryKey          [][]string          `json:"primary_key"`
}
