#!/bin/bash
# build.sh — use Go 1.24.9
GOROOT=/tmp/go1.24.9
exec $GOROOT/bin/go "$@"
