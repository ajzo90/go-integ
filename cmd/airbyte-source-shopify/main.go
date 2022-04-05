package main

import (
	"github.com/ajzo90/go-integ/integrations/shopify"
	"github.com/ajzo90/go-integ/pkg/airbyte"
)

func main() {
	airbyte.Source(shopify.Source)
}
