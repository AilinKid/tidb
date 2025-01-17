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
	"math"
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
	"github.com/pingcap/tidb/pkg/planner/util/cascadesusage"
	"github.com/pingcap/tidb/pkg/planner/util/coreusage"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/intest"
)

var _ rule.Rule = &XFPullUpCorrPredicateFromAgg2{}

// XFPullUpCorrPredicateFromAgg2 pull the correlated expression from projection as child of apply.
type XFPullUpCorrPredicateFromAgg2 struct {
	*XFDeCorrelateApplyBase
}

// NewXFPullUpCorrPredicateFromAgg2 creates a new XFPullUpCorrPredicateFromAgg rule.
func NewXFPullUpCorrPredicateFromAgg2() *XFPullUpCorrPredicateFromAgg2 {
	aggPa := pattern.NewPattern(pattern.OperandDataSource, pattern.EngineTiDBOnly)
	aggPa.SetChildren(pattern.NewPattern(pattern.OperandSelection, pattern.EngineTiDBOnly))

	pa := pattern.NewPattern(pattern.OperandApply, pattern.EngineTiDBOnly)
	pa.SetChildren(pattern.NewPattern(pattern.OperandAny, pattern.EngineTiDBOnly), aggPa)

	return &XFPullUpCorrPredicateFromAgg2{
		XFDeCorrelateApplyBase: &XFDeCorrelateApplyBase{rule.NewBaseRule(rule.XFPullCorrPredFromAgg2, pa)},
	}
}

// ID implements the Rule interface.
func (*XFPullUpCorrPredicateFromAgg2) ID() uint {
	return uint(rule.XFPullCorrPredFromAgg2)
}

// XForm implements the Rule interface.
func (*XFPullUpCorrPredicateFromAgg2) XForm(applyGE base.LogicalPlan) ([]base.LogicalPlan, bool, error) {
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
	sel := innerPlanGE.Children()[0].GetWrappedLogicalPlan().(*logicalop.LogicalSelection)
	intest.Assert(sel != nil)
	clonedSel := *sel

	// We can pull up the equal conditions below the aggregation as the join key of the apply, if only
	// the equal conditions contain the correlated column of this apply.
	if clonedApply.JoinType == logicalop.LeftOuterJoin {
		var (
			eqCondWithCorCol []*expression.ScalarFunction
			remainedExpr     []expression.Expression
		)
		// Extract the equal condition.
		for _, cond := range clonedSel.Conditions {
			// no need to clone the cond, since it will create a new one out.
			if expr := clonedApply.DeCorColFromEqExpr(cond); expr != nil {
				eqCondWithCorCol = append(eqCondWithCorCol, expr.(*expression.ScalarFunction))
			} else {
				remainedExpr = append(remainedExpr, cond)
			}
		}
		if len(eqCondWithCorCol) > 0 {
			clonedSel.Conditions = remainedExpr
			clonedApply.CorCols = coreusage.ExtractCorColumnsBySchema4LogicalPlan(&clonedAgg, outerPlanGE.Schema())
			// There's no other correlated column.
			groupByCols := expression.NewSchema(clonedAgg.GetGroupByCols()...)
			if len(clonedApply.CorCols) == 0 {
				// clone EqualCondition inside clonedApply, because it will be later modified.
				clonedApply.LogicalJoin.EqualConditions = util.CloneScalarFuncs(clonedApply.LogicalJoin.EqualConditions)
				join := &clonedApply.LogicalJoin
				join.EqualConditions = append(join.EqualConditions, eqCondWithCorCol...)

				// clonedAgg schema should be cloned, because it will be later modified.
				clonedAggSchema := clonedAgg.Schema().Clone()
				for _, col := range clonedAggSchema.Columns {
					col.RetType = col.RetType.Clone()
				}
				clonedAgg.SetSchema(clonedAggSchema)
				clonedAgg.AggFuncs = cascadesusage.CloneAggDescs(clonedAgg.AggFuncs)
				clonedAgg.GroupByItems = util.CloneExprs(clonedAgg.GroupByItems)

				for _, eqCond := range eqCondWithCorCol {
					clonedCol := eqCond.GetArgs()[1].(*expression.Column)
					// If the join key is not in the aggregation's schema, add first row function.
					if clonedAgg.Schema().ColumnIndex(eqCond.GetArgs()[1].(*expression.Column)) == -1 {
						newFunc, err := aggregation.NewAggFuncDesc(clonedApply.SCtx().GetExprCtx(), ast.AggFuncFirstRow, []expression.Expression{clonedCol}, false)
						if err != nil {
							return nil, false, err
						}
						clonedAgg.AggFuncs = append(clonedAgg.AggFuncs, newFunc)
						clonedAgg.Schema().Append(clonedCol)
						clonedAgg.Schema().Columns[clonedAgg.Schema().Len()-1].RetType = newFunc.RetTp
					}
					// If group by cols don't contain the join key, add it into this.
					if !groupByCols.Contains(clonedCol) {
						clonedAgg.GroupByItems = append(clonedAgg.GroupByItems, clonedCol)
						groupByCols.Append(clonedCol)
					}
				}
				// The selection may be useless, check and remove it.
				if len(clonedSel.Conditions) == 0 {
					selChildGE := clonedSel.Children()[0]
					clonedAgg.SetChildren(selChildGE)
				}
				defaultValueMap := aggDefaultValueMap(&clonedAgg)
				// We should use it directly, rather than building a projection.
				if len(defaultValueMap) > 0 {
					proj := logicalop.LogicalProjection{}.Init(clonedAgg.SCtx(), clonedAgg.QueryBlockOffset())
					proj.SetSchema(clonedApply.Schema())
					proj.Exprs = expression.Column2Exprs(clonedApply.Schema().Columns)
					for i, val := range defaultValueMap {
						pos := proj.Schema().ColumnIndex(clonedAgg.Schema().Columns[i])
						ifNullFunc := expression.NewFunctionInternal(clonedAgg.SCtx().GetExprCtx(), ast.Ifnull, types.NewFieldType(mysql.TypeLonglong), clonedAgg.Schema().Columns[i], val)
						proj.Exprs[pos] = ifNullFunc
					}
					proj.SetChildren(&clonedApply)
					// proj - clonedApply - clonedAgg - clonedSel - GE
					// proj - clonedApply - clonedAgg - GE
					return []base.LogicalPlan{proj}, false, nil
				}
				// clonedApply - clonedAgg - clonedSel - GE
				// clonedApply - clonedAgg - GE
				return []base.LogicalPlan{&clonedApply}, false, nil
			}
		}
	}
	return nil, false, nil
}

func aggDefaultValueMap(agg *logicalop.LogicalAggregation) map[int]*expression.Constant {
	defaultValueMap := make(map[int]*expression.Constant, len(agg.AggFuncs))
	for i, f := range agg.AggFuncs {
		switch f.Name {
		case ast.AggFuncBitOr, ast.AggFuncBitXor, ast.AggFuncCount:
			defaultValueMap[i] = expression.NewZero()
		case ast.AggFuncBitAnd:
			defaultValueMap[i] = &expression.Constant{Value: types.NewUintDatum(math.MaxUint64), RetType: types.NewFieldType(mysql.TypeLonglong)}
		}
	}
	return defaultValueMap
}
