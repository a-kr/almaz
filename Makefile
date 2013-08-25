.PHONY: almaz test

export GOPATH:=$(shell pwd)

almaz:
	go install almaz

test:
	go test almaz
