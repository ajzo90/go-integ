package storm

import (
	"github.com/ajzo90/go-integ"
)

var orders = integ.NonIncremental("orders", struct {
	Id        int32
	OrderNo   float64
	OrderDate string
}{}).Primary(integ.Field("Id"))

var customers = integ.NonIncremental("users", struct {
	Id           int32
	Key          string
	EmailAddress string
	IsActive     bool
}{}).Primary(integ.Field("Id"))

var items = integ.NonIncremental("items", struct {
	StatusId  int
	PartNo    string
	IsBuyable bool
	Product   Product
}{})

type Product struct {
	Id                 int
	ManufacturerId     int
	ManufacturerPartNo string
}
