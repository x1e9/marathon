#!/bin/bash

artifact_path=$(ls -1 target/universal/marathon-*.tgz)
artifact=$(basename "$artifact_path")
artifact_version="$(echo $artifact | sed -E 's/marathon-(.*)\.tgz/\1/')"

echo "::set-output name=path::$artifact_path"
echo "::set-output name=artifact::$artifact"
echo "::set-output name=version::$artifact_version"
