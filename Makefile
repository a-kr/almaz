.PHONY: almaz test

export GOPATH:=$(shell pwd)

almaz:
	go install almaz

run: almaz
	bin/almaz --audit

test:
	go test almaz
