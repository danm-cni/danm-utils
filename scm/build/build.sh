#!/bin/sh -ex
export GOOS=linux
# Force turn off CGO enables building pure static binaries, otherwise
# built binary still depends on and dinamically linked against the build
# environments standard library implementation (e.g. glibc/musl/...)
export CGO_ENABLED=0
cd "${GOPATH}/src/github.com/danm-cni/danm-utils"
go mod vendor

go install -mod=vendor -a -ldflags "-extldflags '-static'" github.com/danm-cni/danm-utils/cmd/...

