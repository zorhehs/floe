#!/usr/bin/env bash
# The one way to run the Iceberg maintenance job: used by the Airflow DAGs, by
# `make maintenance-now` and by the integration test, so the Spark sizing and the
# retention defaults live in exactly one place. Arguments are passed to maintenance.py.
#   maintenance.sh --compact --rewrite-deletes --rewrite-manifests --expire --orphans
set -euo pipefail
exec spark-submit --master 'local[2]' --driver-memory 1200m \
  --conf spark.ui.enabled=false \
  --conf 'spark.driver.extraJavaOptions=-Daws.region=us-east-1 -XX:MaxDirectMemorySize=256m -XX:MaxMetaspaceSize=256m' \
  /opt/jobs/maintenance.py --retain-last "${RETAIN_LAST:-10}" "$@"
