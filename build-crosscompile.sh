#!/bin/sh

export CGO_ENABLED=1

# The following is for cross-compiling to linux on an arm Mac. Modify to suit your needs.

CC=x86_64-linux-musl-gcc CXX=x86_64-linux-musl-g++ GOARCH=amd64 GOOS=linux CGO_ENABLED=1 go build -ldflags "-linkmode external -extldflags -static"

