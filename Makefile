default: install

install:
	go install github.com/kevinbin/mysqldba
linux:
	env GOOS=linux GOARCH=386 go build -o mysqldba