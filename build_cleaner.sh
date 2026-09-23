#!/bin/sh -e

echo 'Building DANM utils builder container'
docker build --pull --target=builder --tag=utils-builder:1.0 -f scm/build/Dockerfile .

echo 'Building DANM cleaner image and binary'
docker build --pull --target=cleaner --tag="danm-utils:${LATEST_TAG:-$(git describe --tags --dirty 2>/dev/null)}" -f scm/build/Dockerfile .
docker run --rm --net=host --name=utils-builder -v ${GOPATH}/bin:/go/bin -v ${PWD}:/go/src/github.com/danm-cni/danm-utils utils-builder:1.0

echo 'Cleaning up DANM utils builder container'
docker rmi -f utils-builder:1.0

echo 'DANM utils libraries successfully built!'
