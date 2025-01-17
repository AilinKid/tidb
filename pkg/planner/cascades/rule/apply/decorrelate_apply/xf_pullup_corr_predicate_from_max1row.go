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
	"github.com/pingcap/tidb/pkg/planner/cascades/pattern"
	"github.com/pingcap/tidb/pkg/planner/cascades/rule"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/util/intest"
	"strings"
)

var _ rule.Rule = &XFPullUpCorrPredicateFromMax1Row{}

// XFPullUpCorrPredicateFromMax1Row pull the correlated expression from projection as child of apply.
type XFPullUpCorrPredicateFromMax1Row struct {
	*XFDeCorrelateApplyBase
}

// NewXFPullUpCorrPredicateFromMax1Row creates a new XFPullUpCorrPredicateFromMax1Row rule.
func NewXFPullUpCorrPredicateFromMax1Row() *XFPullUpCorrPredicateFromMax1Row {
	pa := pattern.NewPattern(pattern.OperandApply, pattern.EngineTiDBOnly)
	pa.SetChildren(pattern.NewPattern(pattern.OperandAny, pattern.EngineTiDBOnly), pattern.NewPattern(pattern.OperandDataSource, pattern.EngineTiDBOnly))
	return &XFPullUpCorrPredicateFromMax1Row{
		XFDeCorrelateApplyBase: &XFDeCorrelateApplyBase{rule.NewBaseRule(rule.XFPullCorrPredFromMax1Row, pa)},
	}
}

// ID implements the Rule interface.
func (*XFPullUpCorrPredicateFromMax1Row) ID() uint {
	return uint(rule.XFPullCorrPredFromMax1Row)
}

// XForm implements the Rule interface.
func (*XFPullUpCorrPredicateFromMax1Row) XForm(applyGE base.LogicalPlan) ([]base.LogicalPlan, bool, error) {
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
	max1Row := innerPlanGE.GetWrappedLogicalPlan().(*logicalop.LogicalMaxOneRow)
	intest.Assert(max1Row != nil)
	clonedMax1Row := *max1Row

	if !clonedMax1Row.Children()[0].MaxOneRow() {
		return nil, false, nil
	}
	// when inner side max1Row's child is still a max1Row, pull it up.
	max1RowChildGE := clonedMax1Row.Children()[0]
	clonedApply.SetChildren(outerPlanGE, max1RowChildGE)
	return []base.LogicalPlan{&clonedApply}, remove, nil
}
