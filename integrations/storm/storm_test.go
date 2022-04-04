package storm

import (
	"context"
	"log"
	"testing"

	"github.com/ajzo90/go-requests"
	"github.com/matryer/is"
)

func TestMaidn(t *testing.T) {
	req := requests.New("https://query.lab.storm.io/2.0").
		Path("Orders/Orders").
		BasicAuth("21", "9704EACD-1DDB-41EB-8501-4F429E9E1983").
		Query("$select", "Id,OrderNo,OrderDate,BuyerCompanyCode").
		// Query("$filter", "OrderDate gt 2020-06-01T00:00:00.00Z").
		Query("$expand", "Items($select=LineNumber,PartNo,ProductName,QtyOrdered,LineAmount)")
	// Query("$top", "2").
	// Query("$skip", "0")
	// Query("$inlinecount", "allpages")

	resp, err := req.ExecJSON(context.Background())
	is.New(t).NoErr(err)

	log.Println(string(resp.Body().MarshalTo(nil)))
}
