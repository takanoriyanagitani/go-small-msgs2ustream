#!/bin/bash

bin=./cmd/small-msgs2ustream/small-msgs2ustream

export REMOTE_PATH="./test.remote.sock"
export LOCAL_PATH="./test.local.sock"

test -S "${REMOTE_PATH}" || exec env rmt="${REMOTE_PATH}" sh -c '
	echo remote sock "${rmt}" missing.
	exit 1
'

test -S "${LOCAL_PATH}" && exec env lcl="${LOCAL_PATH}" sh -c '
	echo local sock "${lcl}" already exists.
	echo please remove it to run this example.
	exit 1
'

"${bin}"
