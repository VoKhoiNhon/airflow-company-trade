#!/bin/bash

# Parsing absolute filepath of this script
abs_filepath=$(readlink -f $0)
abs_dirpath=$(dirname $abs_filepath)
build_dirpath=$(dirname $abs_dirpath)

#################
# Build Image
#################
build_image() {
DOCKER_BUILDKIT=1 docker build -t apache/airflow:2.10.3-python3.10-custom -f $build_dirpath/Dockerfile --target airflow_custom $build_dirpath
}
##################
# Clean None Image
##################
clean_image() {
docker images | awk '$1 == "<none>" && $2 == "<none>" {print $3}' | xargs -I {} docker image rm {}
}

main() {
    build_image
    clean_image
}
main

