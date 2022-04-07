package klaviyoexclusive

import "github.com/ajzo90/go-integ"

var Incremental, Field = integ.Incremental, integ.Field

//var id = Field("id")

var exclusions = Incremental("exclusions", struct {
	Object   string       `json:"object"`
	Data     []DataObject `json:"data"`
	Page     int          `json:"page"`
	Start    int          `json:"start"`
	End      int          `json:"end"`
	Total    int          `json:"total"`
	PageSize int          `json:"page_size"`
}{})

type DataObject struct {
	Email  string `json:"email"`
	Reason string `json:"reason"`
}
