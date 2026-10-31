.PHONY: all build test test-race bench clean run-visualizer

all: test-race build

build:
	@mkdir -p bin
	go build -o bin/quorumd ./cmd/quorumd
	go build -o bin/visualizer ./cmd/visualizer

test:
	go test -v ./...

test-race:
	go test -v -race ./...

bench:
	go test -bench=. -benchmem ./...

clean:
	rm -rf bin/ data/ *.wal *.snap *.test

run-visualizer: build
	./bin/visualizer -port 8090 -nodes 5
