// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package task

import "github.com/pingcap/tidb/pkg/planner/cascades/memo"

type OptimizeGroupTask struct {
	BaseTask

	group *memo.Group
}

func (t *OptimizeGroupTask) execute() error {
	for e := t.group.LogicalExpressions.Front(); e != nil; e = e.Next() {
		ge := e.Value.(*memo.GroupExpression)
		// Recursively optimize the children of this GroupExpression.
		for _, childGroup := range ge.Children {
			childTask := &OptimizeGroupTask{group: childGroup}
			t.pushTask(childTask)
		}
	}
}
