LINTER := golangci-lint@v1.50.1


lint:
	go run github.com/golangci/golangci-lint/cmd/${LINTER} run

lintfix:
	go run github.com/golangci/golangci-lint/cmd/${LINTER} run --fix