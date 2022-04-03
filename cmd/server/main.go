package main

import (
	"encoding/json"
	"github.com/ajzo90/go-integ/pkg/shopify"
	"log"
	"net/http"
	"strings"
)

func main() {

	var loaders = map[string]http.Handler{
		"shopify":  shopify.Loader,
		"shopify2": shopify.Loader,
	}

	var h = http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if strings.HasPrefix(request.URL.Path, "/discover") {
			var o []string
			for k := range loaders {
				o = append(o, k)
			}
			if err := json.NewEncoder(writer).Encode(o); err != nil {
				http.Error(writer, err.Error(), http.StatusInternalServerError)
			}
			return
		}

		for key, f := range loaders {
			if strings.HasPrefix(request.URL.Path, "/"+key+"/") {
				f.ServeHTTP(writer, request)
				return
			}
		}
		http.Error(writer, "", http.StatusMethodNotAllowed)
	})

	log.Println(http.ListenAndServe(":9900", h))

}
