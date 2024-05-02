#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

${SCRIPT_DIR}/init_sync.sh

python slave_poll.py


