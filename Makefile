.PHONY: test test-coverage gen

test:
	go test -v ./...

test-coverage:
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

gen:
	mockery --config .mockery.yml
