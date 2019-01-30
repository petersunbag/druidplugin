all: grunt build

grunt:
	grunt

build:
	go build -o ./dist/druid-plugin_linux_amd64 .
