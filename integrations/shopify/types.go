package shopify

import (
	"github.com/ajzo90/go-integ"
)

var id = integ.Field("id")

var users = integ.Incremental("users", struct {
	Id               int    `json:"id"`
	Email            string `json:"email"`
	CreatedAt        string `json:"created_at"`
	UpdatedAt        string `json:"updated_at"`
	VerifiedEmail    bool   `json:"verified_email"`
	AcceptsMarketing bool   `json:"accepts_marketing"`
}{}).Primary(id)

var orders = integ.Incremental("orders", struct {
	Id        string  `json:"id"`
	Price     float64 `json:"price"`
	UpdatedAt string  `json:"updated_at"`
}{}).Primary(id)
