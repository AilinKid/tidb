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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/planner/cascades/memo"
	"github.com/pingcap/tidb/pkg/planner/cascades/rule"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/pattern"
	"github.com/pingcap/tidb/pkg/sessionctx"
)

type BaseRule struct {
	pattern *pattern.Pattern
}

type Rule interface {
	Pattern() *pattern.Pattern
	Check(holder *rule.GroupExprHolder, ctx SchedulerContext) bool
	XForm(holder *rule.GroupExprHolder, ctx SchedulerContext) (substitutions []base.LogicalPlan)
}

type MemoContext struct {
	sStx  sessionctx.Context
	stack *taskStack
}

func NewMemoContext(sctx sessionctx.Context) *MemoContext {
	return &MemoContext{sctx, StackTaskPool.Get().(*taskStack)}
}

func (m *MemoContext) destroy() {
	m.stack.Destroy()
}

func (m *MemoContext) getStack() Stack {
	return m.stack
}

func (m *MemoContext) pushTask(task Task) {
	m.stack.Push(task)
}

type BaseTask struct {
	mm   *memo.Memo
	mCtx SchedulerContext
}

// Desc implements the Task interface.
func (b *BaseTask) Desc() string {
	return "base task"
}

// Execute implements the Task interface.
func (b *BaseTask) Execute() error {
	return errors.NewNoStackError("base task should not be executed")
}

func (b *BaseTask) Push(t Task) {
	b.mCtx.pushTask(t)
}

type ApplyRuleTask struct {
	BaseTask

	gE         *memo.GroupExpression
	rule       Rule
	isExplored bool
}

func NewApplyRuleTask(mm *memo.Memo, mCtx SchedulerContext, rule Rule, gE *memo.GroupExpression) Task {
	return &ApplyRuleTask{
		BaseTask: BaseTask{mm, mCtx},
		gE:       gE,
		rule:     rule,
	}
}

func (a *ApplyRuleTask) Execute() error {
	if a.isExplored {
		return nil
	}
	pa := a.rule.Pattern()
	binder := rule.NewBinder(pa, a.gE)
	for binder.Next() {
		holder := binder.GetHolder()
		if !a.rule.Check(holder, a.mCtx) {
			continue
		}
		substitutions := a.rule.XForm(holder, a.mCtx)
		for _, sub := range substitutions {
			newGroupExpr, ok := a.mm.CopyIn(a.gE.GetGroup(), sub)
			if !ok {
				continue
			}
			// YAMS only care about logical plan.
			a.Push(NewOptExpressionTask(a.mm, a.mCtx, newGroupExpr))
		}
	}
}
