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
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/cascades/pattern"
	"github.com/pingcap/tidb/pkg/planner/cascades/rule"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/util/intest"
	"strings"
)

var _ rule.Rule = &XFPullUpCorrPredicateFromDS{}

// XFPullUpCorrPredicateFromDS pull the correlated expression from projection as child of apply.
type XFPullUpCorrPredicateFromDS struct {
	*XFDeCorrelateApplyBase
}

// NewXFPullUpCorrPredicateFromDS creates a new XFPullUpCorrPredicateFromSel rule.
func NewXFPullUpCorrPredicateFromDS() *XFPullUpCorrPredicateFromDS {
	pa := pattern.NewPattern(pattern.OperandApply, pattern.EngineTiDBOnly)
	pa.SetChildren(pattern.NewPattern(pattern.OperandAny, pattern.EngineTiDBOnly), pattern.NewPattern(pattern.OperandDataSource, pattern.EngineTiDBOnly))
	return &XFPullUpCorrPredicateFromDS{
		XFDeCorrelateApplyBase: &XFDeCorrelateApplyBase{rule.NewBaseRule(rule.XFPullCorrPredFromDS, pa)},
	}
}

// ID implements the Rule interface.
func (*XFPullUpCorrPredicateFromDS) ID() uint {
	return uint(rule.XFPullCorrPredFromDS)
}

// XForm implements the Rule interface.
func (*XFPullUpCorrPredicateFromDS) XForm(applyGE base.LogicalPlan) ([]base.LogicalPlan, bool, error) {
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
	ds := innerPlanGE.GetWrappedLogicalPlan().(*logicalop.DataSource)
	intest.Assert(ds != nil)
	clonedDS := *ds

	// extract those predicates containing the correlated column from outer schema.
	corrPreds := make([]expression.Expression, 0, len(clonedDS.AllConds))
	for i := len(clonedDS.AllConds) - 1; i >= 0; i-- {
		cond := clonedDS.AllConds[i]
		if expression.ContainCorrelatedColumn([]expression.Expression{cond}) {
			clonedDS.AllConds = append(clonedDS.AllConds[:i], clonedDS.AllConds[i+1:]...)
			corrPreds = append(corrPreds, cond)
		}
	}
	for i := len(clonedDS.PushedDownConds) - 1; i >= 0; i-- {
		cond := clonedDS.PushedDownConds[i]
		if expression.ContainCorrelatedColumn([]expression.Expression{cond}) {
			clonedDS.PushedDownConds = append(clonedDS.PushedDownConds[:i], clonedDS.PushedDownConds[i+1:]...)
			corrPreds = append(corrPreds, cond)
		}
	}
	expression.RemoveDupExprs(corrPreds)
	if corrPreds == nil {
		return nil, false, nil
	}
	// de-correlate correlated conditions.
	for i := 0; i < len(corrPreds); i++ {
		corrPreds[i] = corrPreds[i].Decorrelate(outerPlanGE.Schema())
	}
	// pull those correlated conditions to the new apply.
	clonedApply.AttachOnConds(corrPreds)
	// clonedDS is the new leaf node, doesn't belong to any existed group, because conditions has changed.
	clonedApply.SetChildren(outerPlanGE, &clonedDS)
	return []base.LogicalPlan{&clonedApply}, remove, nil
}
