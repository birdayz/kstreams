tidy: 
	find . -name go.mod -not -path "*/.*" | xargs dirname |  xargs -i sh -c 'cd {} && go mod tidy'
test:
	find . -name go.mod -not -path "*/.*" | xargs dirname |  xargs -i sh -c 'cd {} && go test -v ./...'
lint:
	golangci-lint run
