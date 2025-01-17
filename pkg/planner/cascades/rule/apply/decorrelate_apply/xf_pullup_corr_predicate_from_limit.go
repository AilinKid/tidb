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

var _ rule.Rule = &XFPullUpCorrPredicateFromLimit{}

// XFPullUpCorrPredicateFromLimit pull the correlated expression from projection as child of apply.
type XFPullUpCorrPredicateFromLimit struct {
	*XFDeCorrelateApplyBase
}

// NewXFPullUpCorrPredicateFromLimit creates a new XFPullUpCorrPredicateFromLimit rule.
func NewXFPullUpCorrPredicateFromLimit() *XFPullUpCorrPredicateFromLimit {
	pa := pattern.NewPattern(pattern.OperandApply, pattern.EngineTiDBOnly)
	pa.SetChildren(pattern.NewPattern(pattern.OperandAny, pattern.EngineTiDBOnly), pattern.NewPattern(pattern.OperandDataSource, pattern.EngineTiDBOnly))
	return &XFPullUpCorrPredicateFromLimit{
		XFDeCorrelateApplyBase: &XFDeCorrelateApplyBase{rule.NewBaseRule(rule.XFPullCorrPredFromLimit, pa)},
	}
}

// ID implements the Rule interface.
func (*XFPullUpCorrPredicateFromLimit) ID() uint {
	return uint(rule.XFPullCorrPredFromLimit)
}

// XForm implements the Rule interface.
func (*XFPullUpCorrPredicateFromLimit) XForm(applyGE base.LogicalPlan) ([]base.LogicalPlan, bool, error) {
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
	limit := innerPlanGE.GetWrappedLogicalPlan().(*logicalop.LogicalLimit)
	intest.Assert(limit != nil)
	clonedLimit := *limit

	// The presence of 'limit' in 'exists' will make the plan not optimal, so we need to decorrelate the 'limit' of subquery in optimization.
	// e.g. select count(*) from test t1 where exists (select value from test t2 where t1.id = t2.id limit 1); When using 'limit' in subquery, the plan will not optimal.
	// If apply is not SemiJoin, the output of it might be expanded even though we are `limit 1`.
	if apply.JoinType != logicalop.SemiJoin && apply.JoinType != logicalop.LeftOuterSemiJoin && apply.JoinType != logicalop.AntiSemiJoin && apply.JoinType != logicalop.AntiLeftOuterSemiJoin {
		return nil, false, nil
	}
	// If subquery has some filter condition, we will not optimize limit.
	if len(apply.LeftConditions) > 0 || len(apply.RightConditions) > 0 || len(apply.OtherConditions) > 0 || len(apply.EqualConditions) > 0 {
		return nil, false, nil
	}
	// Limit with non-0 offset will conduct an impact of itself on the final result set from its sub-child, consequently determining the bool value of the exist subquery.
	if clonedLimit.Offset != 0 {
		return nil, false, nil
	}
	limitChildGE := clonedLimit.Children()[0]
	clonedApply.SetChildren(outerPlanGE, limitChildGE)
	return []base.LogicalPlan{&clonedApply}, remove, nil
}
