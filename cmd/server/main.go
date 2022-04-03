package main

import (
	"encoding/json"
	"fmt"
	"github.com/ajzo90/go-integ/pkg/integ"
	"github.com/ajzo90/go-integ/pkg/shopify"
	"log"
	"net/http"
	"strings"
)

func main() {

	var loaders = map[string]integ.Loader{
		"shopify": shopify.Loader,
	}
	for name, loader := range loaders {
		if err := loader.Validate(); err != nil {
			panic(fmt.Errorf("validation error in %s: %v", name, err))
		}
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
