#!/bin/bash
set -e

# Start data coordinator locally and build it if necessary
REALPATH=$(python -c "import os; print(os.path.realpath('$0'))")
BINDIR=$(dirname "$REALPATH")

JARFILE=$("$BINDIR"/build.sh "$@")

java -Djava.net.preferIPv4Stack=true -jar "$JARFILE"
