package klaviyomembers

import "github.com/ajzo90/go-integ"

var Incremental, Field = integ.Incremental, integ.Field

var profiles = Incremental("profiles", struct {
	Records []profile `json:"records"`
	Marker  int       `json:"marker"`
}{})

type profile struct {
	ID          string `json:"id"`
	Email       string `json:"email"`
	PhoneNumber string `json:"phone_number,omitempty"`
	PushToken   string `json:"push_token,omitempty"`
}
