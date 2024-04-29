#!/bin/bash
#
# Copyright 2024 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -eux

check_cluster_version 5 2 0 'duplicate detection' || exit 0

mydir=$(dirname "${BASH_SOURCE[0]}")

run_sql 'DROP TABLE IF EXISTS dup_resolve.a'
run_sql 'DROP TABLE IF EXISTS lightning_task_info.conflict_error_v3'

! run_lightning --backend local --config "${mydir}/config.toml"
[ $? -eq 0 ]

tail -n 10 $TEST_DIR/lightning.log | grep "ERROR" | tail -n 1 | grep -Fq "[Lightning:Restore:ErrFoundIndexConflictRecords]found index conflict records in table a, index name is 'a.key_b', unique key is '[101]', primary key is '1'"

check_not_contains "the whole procedure completed" $TEST_DIR/lightning.log
