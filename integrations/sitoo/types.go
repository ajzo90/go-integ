package sitoo

import "github.com/ajzo90/go-integ"

var Incremental, Field = integ.Incremental, integ.Field

//var id = Field("userid")

var users = Incremental("users", struct {
	Users      []User `json:"items"`
	TotalCount int    `json:"totalcount"`
}{})

type User struct {
	Userid       string `json:"userid"`
	Email        string `json:"email"`
	Namefirst    string `json:"namefirst"`
	Namelast     string `json:"namelast"`
	Company      string `json:"company"`
	Datecreated  int    `json:"datecreated"`
	Datemodified int    `json:"datemodified"`
}

var orders = Incremental("orders", struct {
	Orders     []Order `json:"items"`
	TotalCount int     `json:"totalcount"`
}{})

type Order struct {
	Orderid          int    `json:"orderid"`
	Eshopid          int    `json:"eshopid"`
	Orderdate        int    `json:"orderdate"`
	Commentinternal  string `json:"commentinternal"`
	Customerref      string `json:"customerref"`
	Checkouttypename string `json:"checkouttypename"`
	Deliverytypename string `json:"deliverytypename"`
	Currencycode     string `json:"currencycode"`
}
