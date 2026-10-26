#!/usr/bin/env bash
# Compile and run all tests with nothing but a JDK (17+).   Usage: ./build.sh [--no-tests]
set -euo pipefail
cd "$(dirname "$0")"
case "$(uname -s)" in MINGW*|MSYS*|CYGWIN*) SEP=";" ;; *) SEP=":" ;; esac   # Windows java wants ; in a classpath
rm -rf out && mkdir -p out/main out/test
javac -Xlint:all -d out/main $(find src/main -name '*.java')
if [ "${1:-}" = "--no-tests" ]; then exit 0; fi
javac -Xlint:all -cp out/main -d out/test $(find src/test -name '*.java')
java -cp "out/main${SEP}out/test" dev.samridhi.sqs.testkit.Runner \
  dev.samridhi.sqs.StandardQueueTests dev.samridhi.sqs.FifoQueueTests dev.samridhi.sqs.DeadLetterTests \
  dev.samridhi.sqs.RandomizedInvariantTests dev.samridhi.sqs.ConcurrencyTests dev.samridhi.sqs.WorkerTests \
  dev.samridhi.sqs.HttpServerTests
