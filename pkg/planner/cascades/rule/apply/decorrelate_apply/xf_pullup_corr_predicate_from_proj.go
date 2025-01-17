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
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/cascades/pattern"
	"github.com/pingcap/tidb/pkg/planner/cascades/rule"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/util/intest"
)

var _ rule.Rule = &XFPullUpCorrPredicateFromProj{}

// XFPullUpCorrPredicateFromProj pull the correlated expression from projection as child of apply.
type XFPullUpCorrPredicateFromProj struct {
	*XFDeCorrelateApplyBase
}

// NewXFPullUpCorrPredicateFromProj creates a new XFPullUpCorrPredicateFromProj rule.
func NewXFPullUpCorrPredicateFromProj() *XFPullUpCorrPredicateFromProj {
	pa := pattern.NewPattern(pattern.OperandApply, pattern.EngineTiDBOnly)
	pa.SetChildren(pattern.NewPattern(pattern.OperandAny, pattern.EngineTiDBOnly), pattern.NewPattern(pattern.OperandProjection, pattern.EngineTiDBOnly))
	return &XFPullUpCorrPredicateFromProj{
		XFDeCorrelateApplyBase: &XFDeCorrelateApplyBase{rule.NewBaseRule(rule.XFPullCorrPredFromProj, pa)},
	}
}

// ID implement the Rule interface.
func (*XFPullUpCorrPredicateFromProj) ID() uint {
	return uint(rule.XFPullCorrPredFromProj)
}

// XForm implements thr Rule interface.
// in this case, it will return two type of logical plan
//
// case1:   applyOP (copyIn-anchor)
//
//	        /     \
//	outerPlanGE  projChildGE
//
// since both two children GE has been explored because the bottom-up order, the new applyOP
// will try to find its suitable rule according to what the type of projChildGE is like.
//
// case2:   projOP (copyIn-anchor)
//
//	           |
//	        applyOP (new group with itself GE)
//	        /     \
//	outerPlanGE  projChildGE
//
// all the same for applyOP's children, when we re-insert projOP, it will re-insert applyOP as
// well, which doesn't belong to any group, so a new group will be created. After copyIn action
// is finished, ApplyRuleTask will push a OptGroupExpressionTask for projOP, since it's a new OP,
// and its child Group hasn't been explored, the new OptGroupTask will be triggered and pushed
// as well, which will come back to the same concept as what the case1 shows.
func (*XFPullUpCorrPredicateFromProj) XForm(applyGE base.LogicalPlan) ([]base.LogicalPlan, bool, error) {
	apply := applyGE.GetWrappedLogicalPlan().(*logicalop.LogicalApply)
	remove := apply.HasFlag(logicalop.ApplyGenFromXFDeCorrelateRuleFlag)
	clonedApply := *apply
	if !remove {
		// when the src apply is original one, the cloned one should be set with flag to indicate
		// it's an intermediary apply operator to avoid unnecessary rules when exploring them.
		clonedApply.SetFlag(logicalop.ApplyGenFromXFDeCorrelateRuleFlag)
	}
	// clone, since baseLogicalPlan is struct usage inside apply, it will be renewed.
	outerPlanGE := applyGE.Children()[0]
	innerPlanGE := applyGE.Children()[1]
	proj := innerPlanGE.GetWrappedLogicalPlan().(*logicalop.LogicalProjection)
	intest.Assert(proj != nil)
	clonedProj := *proj

	// After the column pruning, some expressions in the projection operator may be pruned.
	// In this situation, we can decorrelate the apply operator.
	allConst := len(clonedProj.Exprs) > 0
	for _, expr := range clonedProj.Exprs {
		if len(expression.ExtractCorColumns(expr)) > 0 || !expression.ExtractColumnSet(expr).IsEmpty() {
			allConst = false
			break
		}
	}
	if allConst && clonedApply.JoinType == logicalop.LeftOuterJoin {
		// If the projection just references some constant. We cannot directly pull it up when the APPLY is an outer join.
		//  e.g. select (select 1 from t1 where t1.a=t2.a) from t2; When the t1.a=t2.a is false the join's output is NULL.
		//       But if we pull the projection upon the APPLY. It will return 1 since the projection is evaluated after the join.
		// We disable the decorrelation directly for now.
		// TODO: Actually, it can be optimized. We need to first push the projection down to the selection. And then the APPLY can be decorrelated.
		return nil, false, nil
	}
	// step1: substitute the all the schema with new expressions (including correlated column maybe, but it doesn't affect the collation infer inside)
	// eg: projection: constant("guo") --> column8, once upper layer substitution failed here, the lower layer behind
	// projection can't supply column8 anymore.
	//
	//	upper OP (depend on column8)   --> projection(constant "guo" --> column8)  --> lower layer OP
	//	          |                                                       ^
	//	          +-------------------------------------------------------+
	//
	//	upper OP (depend on column8)   --> lower layer OP
	//	          |                             ^
	//	          +-----------------------------+      // Fail: lower layer can't supply column8 anymore.
	// the columns substitution inside doesn't affect the original apply.
	hasFail := clonedApply.ColumnSubstituteAll(clonedProj.Schema(), clonedProj.Exprs)
	if hasFail {
		return nil, false, nil
	}
	// step2: when it can be substituted all, we then just do the de-correlation (apply conditions included).
	for i, expr := range clonedProj.Exprs {
		// the column de-correlation inside doesn't affect the original proj.
		clonedProj.Exprs[i] = expr.Decorrelate(outerPlanGE.Schema())
	}
	// the column de-correlation inside doesn't affect the original apply.
	clonedApply.Decorrelate(outerPlanGE.Schema())

	// get the original
	projChildGE := clonedProj.Children()[0]
	clonedApply.SetChildren(outerPlanGE, innerPlanGE)
	if clonedApply.JoinType != logicalop.SemiJoin && clonedApply.JoinType != logicalop.LeftOuterSemiJoin &&
		clonedApply.JoinType != logicalop.AntiSemiJoin && clonedApply.JoinType != logicalop.AntiLeftOuterSemiJoin {
		clonedProj.SetSchema(clonedApply.Schema())
		clonedProj.Exprs = append(expression.Column2Exprs(outerPlanGE.Schema().Clone().Columns), clonedProj.Exprs...)
		clonedApply.SetSchema(expression.MergeSchema(outerPlanGE.Schema(), projChildGE.Schema()))
		// branch1: we still need this proj for inner schema complement, new apply will be set as its children.
		clonedProj.SetChildren(&clonedApply)
		return []base.LogicalPlan{&clonedProj}, false, nil
	}
	// branch2: anti-semi/semi with/without scalar column don't need proj to do the inner schema complement.
	return []base.LogicalPlan{&clonedApply}, remove, nil
}
