#!/bin/sh -ex
export GOOS=linux
# The CNI binaries land on the nodes of a foreign cluster, so everything here is built as a static
# executable, exactly like the other binaries of this repository.
export CGO_ENABLED=0
PAYLOAD_RELEASE=${PAYLOAD_RELEASE:-4.4}

cd "${GOPATH}/src/github.com/danm-cni/danm-utils"
go build -mod=mod -a -ldflags "-extldflags '-static'" -o /go/bin/dma github.com/danm-cni/danm-utils/cmd/dma

# The payload is built from the DANM version pinned in go.mod, so that the binaries installed on the
# nodes come from the release the embedded manifests and schemas come from. It is built in a module
# of its own, since the command packages of DANM are not dependencies of this repository and so are
# neither vendored nor recorded in its go.sum.
DANM_VERSION=$(go list -m -f '{{ .Version }}' github.com/danm-cni/danm)
mkdir -p /build/payload "/payload/${PAYLOAD_RELEASE}"
cd /build/payload
go mod init dma-payload
for binary in danm fakeipam
do
  # The command package is fetched by name, not the module, so that go.sum ends up holding the
  # modules those commands import as well.
  go get "github.com/danm-cni/danm/cmd/${binary}@${DANM_VERSION}"
  go build -mod=mod -a -ldflags "-extldflags '-static'" \
    -o "/payload/${PAYLOAD_RELEASE}/${binary}" \
    "github.com/danm-cni/danm/cmd/${binary}"
done
echo "payload ${PAYLOAD_RELEASE} built from github.com/danm-cni/danm ${DANM_VERSION}"
