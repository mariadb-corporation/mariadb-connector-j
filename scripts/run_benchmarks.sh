#!/usr/bin/env bash
set -euxo pipefail

mvn package -P bench -Dmaven.test.skip -Dmaven.javadoc.skip
java -DTEST_PORT=5506 -DTEST_PASSWORD=password -DTEST_DATABASE=test -jar target/benchmarks.jar
