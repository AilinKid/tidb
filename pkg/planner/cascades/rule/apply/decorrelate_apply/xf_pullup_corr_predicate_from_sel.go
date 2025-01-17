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

package decorrelate_apply

import (
	"fmt"
	"strings"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/cascades/pattern"
	"github.com/pingcap/tidb/pkg/planner/cascades/rule"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/util/intest"
)

var _ rule.Rule = &XFPullUpCorrPredicateFromSel{}

// XFPullUpCorrPredicateFromSel pull the correlated expression from projection as child of apply.
type XFPullUpCorrPredicateFromSel struct {
	*XFDeCorrelateApplyBase
}

// NewXFPullUpCorrPredicateFromSel creates a new XFPullUpCorrPredicateFromSel rule.
func NewXFPullUpCorrPredicateFromSel() *XFPullUpCorrPredicateFromSel {
	pa := pattern.NewPattern(pattern.OperandApply, pattern.EngineTiDBOnly)
	pa.SetChildren(pattern.NewPattern(pattern.OperandAny, pattern.EngineTiDBOnly), pattern.NewPattern(pattern.OperandSelection, pattern.EngineTiDBOnly))
	return &XFPullUpCorrPredicateFromSel{
		XFDeCorrelateApplyBase: &XFDeCorrelateApplyBase{rule.NewBaseRule(rule.XFPullCorrPredFromSel, pa)},
	}
}

// ID implements the Rule interface.
func (*XFPullUpCorrPredicateFromSel) ID() uint {
	return uint(rule.XFPullCorrPredFromSel)
}

// XForm implements the Rule interface.
func (*XFPullUpCorrPredicateFromSel) XForm(applyGE base.LogicalPlan) ([]base.LogicalPlan, bool, error) {
	if strings.Contains(applyGE.SCtx().GetSessionVars().StmtCtx.OriginalSQL, "SELECT 1 FROM t1 AS tab") {
		fmt.Println(1)
	}
	apply := applyGE.GetWrappedLogicalPlan().(*logicalop.LogicalApply)
	remove := apply.HasFlag(logicalop.ApplyGenFromXFDeCorrelateRuleFlag)
	// clone, since baseLogicalPlan is struct usage inside apply, it will be renewed.
	clonedApply := *apply
	if !remove {
		// when the src apply is original one, the cloned one should be set with flag to indicate
		// it's an intermediary apply operator to avoid unnecessary rules when exploring them.
		clonedApply.SetFlag(logicalop.ApplyGenFromXFDeCorrelateRuleFlag)
	}
	outerPlanGE := applyGE.Children()[0]
	innerPlanGE := applyGE.Children()[1]
	sel := innerPlanGE.GetWrappedLogicalPlan().(*logicalop.LogicalSelection)
	intest.Assert(sel != nil)
	clonedSel := *sel

	// If the inner plan is a selection, we add this condition to join predicates.
	// Notice that no matter what kind of join is, it's always right.
	newConds := make([]expression.Expression, 0, len(clonedSel.Conditions))
	for _, cond := range clonedSel.Conditions {
		newConds = append(newConds, cond.Decorrelate(outerPlanGE.Schema()))
	}
	// pull selection's new condition to new apply.
	clonedApply.AttachOnConds(newConds)
	selChildGE := sel.Children()[0]
	clonedApply.SetChildren(outerPlanGE, selChildGE)
	return []base.LogicalPlan{&clonedApply}, remove, nil
}
