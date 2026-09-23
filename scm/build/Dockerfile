# Set defaults
ARG USERNAME=danm
ARG UID=147
ARG BUILDER=golang:1.26-alpine3.24
ARG CALICOCTL_VERSION=v3.32.2

FROM ${BUILDER} AS builder
MAINTAINER Levente Kale <levente.kale@gmail.com>
ARG USERNAME
ARG UID
ARG CALICOCTL_VERSION

RUN apk add --no-cache ca-certificates make git bash

RUN mkdir -p ${GOPATH}/src/github.com/danm-cni/danm-utils
COPY / ${GOPATH}/src/github.com/danm-cni/danm-utils/

WORKDIR ${GOPATH}/src/github.com/danm-cni/danm-utils

RUN scm/build/build.sh \
 && adduser -u ${UID} -D -H -s /sbin/nologin ${USERNAME} \
 && chown root:${USERNAME} /go/bin/cleaner /go/bin/policer \
 && chmod 0750 /go/bin/*
ADD https://github.com/projectcalico/calico/releases/download/${CALICOCTL_VERSION}/calicoctl-linux-amd64 /go/bin/calicoctl
RUN chmod +x /go/bin/calicoctl && chown root:${USERNAME} /go/bin/calicoctl

FROM alpine:latest as cleaner
MAINTAINER Levente Kale <levente.kale@gmail.com>

RUN adduser -u 666 -D -H -s /sbin/nologin cleaner
COPY --from=builder /go/bin/cleaner /go/bin/calicoctl /usr/local/bin/
USER cleaner
WORKDIR /
ENTRYPOINT ["/usr/local/bin/cleaner"]

FROM alpine:latest AS policer
MAINTAINER Levente Kale <levente.kale@gmail.com>
ARG USERNAME
ARG UID
RUN adduser -u ${UID} -D -H -s /sbin/nologin ${USERNAME}
WORKDIR /
USER ${USER}
COPY --from=builder /go/bin/policer /usr/local/bin/policer
RUN apk add --no-cache iptables ip6tables
RUN apk add --no-cache --virtual .tools libcap \
 && setcap cap_net_raw,cap_net_admin,cap_sys_admin=eip /usr/local/bin/policer \
 && apk del .tools
ENTRYPOINT ["/usr/local/bin/policer"]
