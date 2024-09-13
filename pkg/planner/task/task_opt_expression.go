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
)

type OptExpressionTask struct {
	BaseTask

	gE         *memo.GroupExpression
	isExplored bool
}

func (t *OptExpressionTask) Execute() error {
	rules := t.FilterRules()
	for _, rule := range rules {
		t.Push(NewApplyRuleTask(t.mm, t.mCtx, rule, t.gE))
	}
	// 对于孩子新探索过的 group，推导这个表达式新的统计信息。
	// derive stats 我们也不需要做，因为在 PhysicalOpt 的时候会自动推导 stats。
	for _, child := range t.gE.Inputs {
		if !child.Explored() {
			t.Push(NewExploreGroupTask(t.mm, t.mCtx, child))
		}
	}
	return nil
}

func (t *OptExpressionTask) FilterRules() []Rule {
	// todo:利用 gE 的 operand 性质过滤规则
	return nil
}

func NewOptExpressionTask(mm *memo.Memo, mCtx SchedulerContext, gE *memo.GroupExpression) Task {
	return &OptExpressionTask{
		BaseTask: BaseTask{mm, mCtx},
		gE:       gE,
	}
}
