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
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/cascades/pattern"
	"github.com/pingcap/tidb/pkg/planner/cascades/rule"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/util/intest"
)

//var _ rule.Rule = &XFPullUpCorrPredicateFromAgg1{}

// XFPullUpCorrPredicateFromAgg1 pull the correlated expression from projection as child of apply.
type XFPullUpCorrPredicateFromAgg1 struct {
	*XFDeCorrelateApplyBase
}

// NewXFPullUpCorrPredicateFromAgg1 creates a new XFPullUpCorrPredicateFromAgg1 rule.
func NewXFPullUpCorrPredicateFromAgg1() *XFPullUpCorrPredicateFromAgg1 {
	pa := pattern.NewPattern(pattern.OperandApply, pattern.EngineTiDBOnly)
	pa.SetChildren(pattern.NewPattern(pattern.OperandAny, pattern.EngineTiDBOnly), pattern.NewPattern(pattern.OperandAggregation, pattern.EngineTiDBOnly))
	return &XFPullUpCorrPredicateFromAgg1{
		XFDeCorrelateApplyBase: &XFDeCorrelateApplyBase{rule.NewBaseRule(rule.XFPullCorrPredFromAgg1, pa)},
	}
}

// ID implements the Rule interface.
func (*XFPullUpCorrPredicateFromAgg1) ID() uint {
	return uint(rule.XFPullCorrPredFromAgg1)
}

// XForm implements the Rule interface.
func (*XFPullUpCorrPredicateFromAgg1) XForm(applyGE base.LogicalPlan) ([]base.LogicalPlan, bool, error) {
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
	agg := innerPlanGE.GetWrappedLogicalPlan().(*logicalop.LogicalAggregation)
	intest.Assert(agg != nil)
	clonedAgg := *agg

	if clonedApply.CanPullUpAgg() && clonedAgg.CanPullUp() {
		// we need deep clone the schema, since we will reset the null flag below.
		clonedApply.SetSchema(clonedApply.Schema().Clone())

		aggChildGE := clonedAgg.Children()[0]
		clonedApply.JoinType = logicalop.LeftOuterJoin
		clonedApply.SetChildren(outerPlanGE, aggChildGE)
		clonedAgg.SetSchema(clonedApply.Schema())
		clonedAgg.GroupByItems = expression.Column2Exprs(outerPlanGE.Schema().PKOrUK[0])
		newAggFuncs := make([]*aggregation.AggFuncDesc, 0, apply.Schema().Len())

		outerColsInSchema := make([]*expression.Column, 0, outerPlanGE.Schema().Len())
		for i, col := range outerPlanGE.Schema().Columns {
			first, err := aggregation.NewAggFuncDesc(agg.SCtx().GetExprCtx(), ast.AggFuncFirstRow, []expression.Expression{col}, false)
			if err != nil {
				return nil, false, nil
			}
			newAggFuncs = append(newAggFuncs, first)

			outerCol, _ := outerPlanGE.Schema().Columns[i].Clone().(*expression.Column)
			outerCol.RetType = first.RetTp
			outerColsInSchema = append(outerColsInSchema, outerCol)
		}
		clonedApply.SetSchema(expression.MergeSchema(expression.NewSchema(outerColsInSchema...), aggChildGE.Schema()))
		// set apply inner side schema as nullable, because currently it's a left outer join type.
		util.ResetNotNullFlag(clonedApply.Schema(), outerPlanGE.Schema().Len(), apply.Schema().Len())

		for i, aggFunc := range clonedAgg.AggFuncs {
			aggArgs := make([]expression.Expression, 0, len(aggFunc.Args))
			for _, arg := range aggFunc.Args {
				switch expr := arg.(type) {
				case *expression.Column:
					// if clonedApply's schema contain clonedAgg's arg column, use the one from apply.
					// since its nullability may have changed.
					if idx := clonedApply.Schema().ColumnIndex(expr); idx != -1 {
						aggArgs = append(aggArgs, apply.Schema().Columns[idx])
					} else {
						aggArgs = append(aggArgs, expr)
					}
				case *expression.ScalarFunction:
					expr.RetType = expr.RetType.Clone()
					expr.RetType.DelFlag(mysql.NotNullFlag)
					aggArgs = append(aggArgs, expr)
				default:
					aggArgs = append(aggArgs, expr)
				}
			}
			desc, err := aggregation.NewAggFuncDesc(agg.SCtx().GetExprCtx(), agg.AggFuncs[i].Name, aggArgs, agg.AggFuncs[i].HasDistinct)
			if err != nil {
				return nil, false, nil
			}
			newAggFuncs = append(newAggFuncs, desc)
		}
		clonedAgg.AggFuncs = newAggFuncs
		clonedAgg.SetChildren(&clonedApply)
		// TODO: Add a Projection if any argument of aggregate funcs or group by items are scalar functions.
		// agg.buildProjectionIfNecessary()
		return []base.LogicalPlan{&clonedAgg}, remove, nil
	}
	return nil, false, nil
}
