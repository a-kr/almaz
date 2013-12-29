.PHONY: almaz test

export GOPATH:=$(shell pwd)

almaz:
	go install almaz

run: almaz
	bin/almaz --persist

test:
	go test almaz

depend:
	go get github.com/gorilla/websocket
