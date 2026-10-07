#!/usr/bin/env bash
# Downloads the extra Spark jars (Iceberg, Kafka connector) into docker/jars/
# with resumable, retried, SHA-1 verified downloads. The Docker build copies
# them from there, so a flaky network never breaks `docker build`.
set -euo pipefail
cd "$(dirname "$0")/../config/docker"
mkdir -p jars

MAVEN=https://repo1.maven.org/maven2
SPARK_VERSION=3.5.6
ICEBERG_VERSION=1.9.2
ARTIFACTS=(
  "org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar"
  "org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"
  "org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar"
  "org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar"
  "org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar"
  "org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar"
  # JDBC driver for the Iceberg REST catalog's PostgreSQL metadata store
  "catalog:org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar"
)

mkdir -p catalog-jars
for entry in "${ARTIFACTS[@]}"; do
  if [[ "$entry" == catalog:* ]]; then path="${entry#catalog:}"; dir=catalog-jars; else path="$entry"; dir=jars; fi
  file="$dir/$(basename "$path")"
  for attempt in $(seq 1 30); do
    expected="$(curl -fsSL --retry 10 --retry-all-errors --connect-timeout 20 "${MAVEN}/${path}.sha1" | awk '{print $1}')" || expected=""
    [[ -n "$expected" ]] || { echo "  sha1 fetch failed, retry $attempt"; sleep 5; continue; }
    if [[ -f "$file" ]] && [[ "$(shasum -a 1 "$file" 2>/dev/null || sha1sum "$file")" == "$expected"* ]]; then
      echo "ok   $file"; break
    fi
    echo "get  $file (attempt $attempt)"
    curl -fL --retry 10 --retry-all-errors --connect-timeout 20 -C - -o "$file" "${MAVEN}/${path}" || true
    if [[ "$(shasum -a 1 "$file" 2>/dev/null || sha1sum "$file")" == "$expected"* ]]; then
      echo "ok   $file"; break
    fi
    # corrupted/partial beyond resume: start over next attempt
    [[ $attempt -ge 3 ]] && rm -f "$file"
    sleep 3
  done
  [[ "$(shasum -a 1 "$file" 2>/dev/null || sha1sum "$file")" == "$expected"* ]] || { echo "FAILED $file"; exit 1; }
done
echo "All jars present and verified."
