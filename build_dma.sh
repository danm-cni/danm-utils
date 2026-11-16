#!/bin/bash -e

build_targets=(dma)
BUILD_COMMAND="docker image build"
TAG_COMMAND="docker image tag"

LATEST_TAG=$(git describe --tags)
COMMIT_HASH=$(git rev-parse --short=8 HEAD)
if [ -n "$(git status --porcelain)" ]
then
  COMMIT_HASH="${COMMIT_HASH}_dirty"
fi

for plugin in ${build_targets[@]}
do
  echo Building: ${plugin}, version ${COMMIT_HASH}
  ${BUILD_COMMAND} \
    --build-arg LATEST_TAG=${LATEST_TAG} \
    --build-arg COMMIT_HASH=${COMMIT_HASH} \
    --tag ${TAG_PREFIX}${plugin}:${COMMIT_HASH} \
    --target ${plugin} \
    --file scm/build/Dockerfile \
    .

  # Tag image as "latest", too
  ${TAG_COMMAND} ${TAG_PREFIX}${plugin}:${COMMIT_HASH} ${TAG_PREFIX}${plugin}:latest
done
