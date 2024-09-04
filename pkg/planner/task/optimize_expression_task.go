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

import (
	"github.com/pingcap/tidb/pkg/planner/cascades/memo"
	"github.com/pingcap/tidb/pkg/planner/cascades/rule"
)

type OptimizeExpressionTask struct {
	BaseTask

	gExpr    *memo.GroupExpression
	explored bool
}

func (t *OptimizeExpressionTask) execute() error {
	if t.explored {
		return nil
	}
	t.explored = true

	for _, childGroup := range t.gExpr.Children {
		childTask := &OptimizeGroupTask{group: childGroup}
		t.pushTask(childTask)
	}
}

func (t *OptimizeExpressionTask) desc() string {
	return "optimize expression"
}

func (t *OptimizeExpressionTask) admissionRules() {
	validRules := make([]rule.BaseRule, 0, rule.NUM_RULES)
	for _, one := range rule.LogicalRules {
		// todo: 1: check whether the rule has been explored?

		// this rule has been explored.
		if t.gExpr.IsExplored(int(one.Type())) {
			continue
		}

		


	}
























