#!/bin/bash -x
#
set -Eeuo pipefail

SCRIPT_DIR=$(realpath $(dirname "$0"))
PROJECT_DIR=$(realpath $(dirname "${SCRIPT_DIR}"))

. "${SCRIPT_DIR}/install-cor.sh"

#----- --- -- -  -  -   -
# Initialize a clean Conan cache/home.
#
export CONAN_HOME="${HOME}/_cache/.conan2"
cor conan profile detect
#----- --- -- -  -  -   -

# Pull configs for this build.
#
cor conan config install --type git https://gitlab.com/batteriesincluded/conan-config/linux-gcc12-x86_64.git

# Build.
#
# cor install makes sure deps are uploaded to the cache server first;
# TODO [tastolfi 2025-09-27] - Add a `cor pre-cache <options> <package_name>/<version> ...` command
#
cor install --clean --profile=linux-gcc12-x86_64 --build-type=Release
cor build   --clean --profile=linux-gcc12-x86_64 --build-type=Release
cor test    --only  --profile=linux-gcc12-x86_64 --build-type=Release
