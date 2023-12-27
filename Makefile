tidy: 
	go list -f '{{.Dir}}' -m | xargs -L1 go mod tidy -C
test:
	go list -f '{{.Dir}}/...' -m | xargs go test
lint:
	golangci-lint run
