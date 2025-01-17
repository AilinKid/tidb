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

	"github.com/pingcap/tidb/pkg/planner/cascades/pattern"
	"github.com/pingcap/tidb/pkg/planner/cascades/rule"
	corebase "github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util/coreusage"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/plancodec"
)

var _ rule.Rule = &XFDeCorrelateSimpleApply{}

// XFDeCorrelateSimpleApply pull the correlated expression from projection as child of apply.
type XFDeCorrelateSimpleApply struct {
	*XFDeCorrelateApplyBase
}

// NewXFDeCorrelateSimpleApply creates a new JoinToApply rule.
func NewXFDeCorrelateSimpleApply() *XFDeCorrelateSimpleApply {
	pa := pattern.NewPattern(pattern.OperandApply, pattern.EngineTiDBOnly)
	pa.SetChildren(pattern.NewPattern(pattern.OperandAny, pattern.EngineTiDBOnly), pattern.NewPattern(pattern.OperandAny, pattern.EngineTiDBOnly))
	return &XFDeCorrelateSimpleApply{
		XFDeCorrelateApplyBase: &XFDeCorrelateApplyBase{rule.NewBaseRule(rule.XFDeCorrelateSimpleApply, pa)},
	}
}

// ID implement the Rule interface.
func (*XFDeCorrelateSimpleApply) ID() uint {
	return uint(rule.XFDeCorrelateSimpleApply)
}

// XForm implements thr Rule interface.
func (*XFDeCorrelateSimpleApply) XForm(applyGE corebase.LogicalPlan) ([]corebase.LogicalPlan, bool, error) {
	if strings.Contains(applyGE.SCtx().GetSessionVars().StmtCtx.OriginalSQL, "SELECT 1 FROM t1 AS tab") {
		fmt.Println(1)
	}
	apply := applyGE.GetWrappedLogicalPlan().(*logicalop.LogicalApply)
	remove := apply.HasFlag(logicalop.ApplyGenFromXFDeCorrelateRuleFlag)
	outerPlanGE := applyGE.Children()[0]
	innerPlanGE := applyGE.Children()[1]
	// modify the apply op's CorCols in-place, it will change the hash64, it should substitute the old one.
	CorCols := coreusage.ExtractCorColumnsBySchema4LogicalPlan(innerPlanGE.GetWrappedLogicalPlan(), outerPlanGE.GetWrappedLogicalPlan().Schema())
	if len(CorCols) == 0 {
		// If the inner plan is non-correlated, this apply will be simplified to join.
		clonedJoin := apply.LogicalJoin
		clonedJoin.SetSelf(&clonedJoin)
		clonedJoin.SetTP(plancodec.TypeJoin)
		// set the new GE's stats to nil, since the inherited stats is not precious, which will be filled in physicalOpt.
		clonedJoin.SetStats(nil)
		intest.Assert(clonedJoin.Children() != nil)
		// we only keep the original apply, while for those generated from intermediate corr-pullUp rules, remove them.
		return []corebase.LogicalPlan{&clonedJoin}, remove, nil
	}
	return nil, false, nil
}
