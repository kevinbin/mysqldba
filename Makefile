default: install

install:
	go install github.com/kevinbin/mydba
linux:
	env GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o mydba