tidy: 
	go list -f '{{.Dir}}' -m | xargs -L1 go mod tidy -C
	go list -f '{{.Dir}}' -m | xargs -L1 go work sync -C
test:
	go list -f '{{.Dir}}/...' -m | xargs go test
lint:
	golangci-lint run
