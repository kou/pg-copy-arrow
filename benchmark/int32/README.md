<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Benchmark - only with 32-bit integer data

## How to run

See the [README.md in the parent directory](../README.md).

## Environment

Here is a benchmark result on the following environment:

- OS: Debian GNU/Linux sid
- CPU: AMD Ryzen 9 3900X 12-Core Processor
- Memory: 64GiB
- PostgreSQL: 17 (not released yet)
  5d8aa8bcedae7376bd97e79052d606db4e4f8dd4
- Apache Arrow: 15.0.0-SNAPSHOT
  63353baf1cda1d1fc7bb614ce01558c12990e073

## Result

![Graph](result.svg)

100K records:

| COPY | SCAN |
| ---- | ---- |
| 99.558 | 66.340 |

1M records:

| COPY | SCAN |
| ---- | ---- |
| 541.505 | 216.474 |

10M records:

| COPY | SCAN |
| ---- | ---- |
| 4839.805 | 1827.057 |
