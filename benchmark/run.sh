#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -eu
set -o pipefail

base_dir=$(dirname "$0")

measure()
{
  local function=$1
  local n_tries=3
  for i in $(seq ${n_tries}); do
    LANG=C \
      PAGER=cat \
      psql \
      --no-psqlrc \
      --command "\\timing" \
      --command "SELECT ${function}('data')" | \
      tail -n 1 | \
      grep -o '[0-9.]* ms' | \
      grep -o '[0-9.]*'
  done | sort --numeric-sort | head -n 1
}

benchmarks=()
benchmarks+=(int32)

for benchmark in "${benchmarks[@]}"; do
  result="${base_dir}/${benchmark}/result.csv"
  echo "Approach,N records,Elapsed time (ms)" | \
    tee "${result}"
  sizes=()
  sizes+=(100000)
  sizes+=(1000000)
  sizes+=(10000000)
  for size in "${sizes[@]}"; do
    export PGDATABASE="copy_arrow_benchmark_${benchmark}_${size}"
    echo "${benchmark}: ${size}: preparing"
    "${base_dir}/${benchmark}/prepare-sql.sh" "${size}" "${PGDATABASE}" | \
      psql -d postgres

    echo "${benchmark}: ${size}: COPY"
    elapsed_time=$(measure copy_to_arrow)
    echo "COPY,${size},${elapsed_time}" | \
      tee -a "${result}"

    echo "${benchmark}: ${size}: SCAN"
    elapsed_time=$(measure scan_to_arrow)
    echo "SCAN,${size},${elapsed_time}" | \
      tee -a "${result}"
  done
done
