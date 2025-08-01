default: install

install:
	go install github.com/kevinbin/mysqldba
linux:
	env GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o mysqldba