#!/usr/bin/env bash
# Downloads the extra Spark jars (Iceberg, Kafka connector) and the PostgreSQL JDBC
# driver into config/docker/jars/ and config/docker/catalog-jars/ with resumable,
# retried downloads. Every jar is pinned to a SHA-1 recorded here, so "verified"
# means "matches this repository", not "matches whatever Maven served today", and a
# checkout with the right jars already present needs no network at all.
set -euo pipefail
cd "$(dirname "$0")/../config/docker"
mkdir -p jars catalog-jars

MAVEN=https://repo1.maven.org/maven2
SPARK_VERSION=3.5.6
ICEBERG_VERSION=1.9.2
# sha1  target-dir  maven-path
ARTIFACTS=(
  "08f897c0683e8c4e0f67eee687bf61aef1709821 jars org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar"
  "29aac0b8b9271dc5aad89563b5eefe130f60739f jars org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"
  "8a74fcf6d4e1c7a23ac296f4279019ba6ff0f64b jars org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar"
  "c82fd1650a7fae1a70dbe5c48fe30beea810283f jars org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar"
  "a2f55251deb3d31a21903a0cfbd4d1fa6b7463ca jars org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar"
  "8970fd110c965f285ed4c6e40be7630c62db6f68 jars org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar"
  # JDBC driver for the Iceberg REST catalog's PostgreSQL metadata store
  "264310fd7b2cd76738787dc0b9f7ea2e3b11adc1 catalog-jars org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar"
)

sha1_of() { if command -v shasum >/dev/null 2>&1; then shasum -a 1 "$1"; else sha1sum "$1"; fi | awk '{print $1}'; }

status=0
for entry in "${ARTIFACTS[@]}"; do
  read -r expected dir path <<<"$entry"
  file="$dir/$(basename "$path")"
  if [[ -f "$file" ]] && [[ "$(sha1_of "$file")" == "$expected" ]]; then
    echo "ok   $file"; continue
  fi
  for attempt in $(seq 1 30); do
    echo "get  $file (attempt $attempt)"
    curl -fL --retry 10 --retry-all-errors --connect-timeout 20 -C - -o "$file" "${MAVEN}/${path}" || true
    [[ -f "$file" ]] && [[ "$(sha1_of "$file")" == "$expected" ]] && { echo "ok   $file"; break; }
    # corrupted/partial beyond resume: start over next attempt
    [[ $attempt -ge 3 ]] && rm -f "$file"
    sleep 3
  done
  if [[ ! -f "$file" ]] || [[ "$(sha1_of "$file")" != "$expected" ]]; then
    echo "FAILED $file (sha1 mismatch or download failed)"; status=1
  fi
done
[[ $status -eq 0 ]] && echo "All jars present and verified."
exit $status
