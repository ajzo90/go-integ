package storm

import (
	"github.com/ajzo90/go-integ"
)

// https://stormdocs.atlassian.net/servicedesk/customer/portal/1/article/2215706817
// https://query.lab.storm.io/2.0/Docs/Index#/Orders/Entities/OrderItem creds(21:XXX)
var orders = integ.Incremental("orders", struct {
	Id        int32
	OrderNo   float64
	OrderDate string
}{}).Primary(integ.Field("Id"))

var customers = integ.Incremental("users", struct {
	Id           int32
	Key          string
	EmailAddress string
	IsActive     bool
}{}).Primary(integ.Field("Id"))

var items = integ.Incremental("items", struct {
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
