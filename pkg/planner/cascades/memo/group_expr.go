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

package memo

import (
	base2 "github.com/pingcap/tidb/pkg/planner/cascades/base"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/pattern"
)

// GroupExpression is a single expression from the equivalent list classes inside a group.
// it is a node in the expression tree, while it takes groups as inputs. This kind of loose
// coupling between Group and GroupExpression is the key to the success of the memory compact
// of representing a forest.
type GroupExpression struct {
	// Group is the Group that this GroupExpression belongs to.
	Group *Group

	// Children stores the Groups that this GroupExpression based on.
	Children []*Group

	// LogicalExpr is internal logical expression stands for this groupExpr.
	LogicalPlan base.LogicalPlan

	// mark is the rule mark of the GroupExpression.
	RuleMark

	// hash64 is the unique fingerprint of the GroupExpression.
	hash64 uint64
}

// RuleMark is uses to mark whether GroupExpr has been fully explored by a transformation rules.
type RuleMark int64

// SetExplored sets the ordinal bit.
func (m *RuleMark) SetExplored(ordinal int) {
	*m |= 1 << ordinal
}

// IsExplored indicates whether the ordinal bit is set.
func (m *RuleMark) IsExplored(ordinal int) bool {
	return *m&(1<<ordinal) > 0
}

// Sum64 returns the cached hash64 of the GroupExpression.
func (e *GroupExpression) Sum64() uint64 {
	return e.hash64
}

// Hash64 implements the Hash64 interface.
func (e *GroupExpression) Hash64(h base2.Hasher) {
	// logical plan hash.
	e.LogicalPlan.Hash64(h)
	// children group hash.
	for _, child := range e.Children {
		child.Hash64(h)
	}
}

// Equals implements the Equals interface.
func (e *GroupExpression) Equals(other any) bool {
	if other == nil {
		return false
	}
	var e2 *GroupExpression
	switch x := other.(type) {
	case *GroupExpression:
		e2 = x
	case GroupExpression:
		e2 = &x
	default:
		return false
	}
	if len(e.Children) != len(e2.Children) {
		return false
	}
	if pattern.GetOperand(e.LogicalPlan) != pattern.GetOperand(e2.LogicalPlan) {
		return false
	}
	// current logical operator meta cmp, logical plan don't care logicalPlan's children.
	// when we convert logicalPlan to GroupExpression, we will set children to nil.
	if !e.LogicalPlan.Equals(e2.LogicalPlan) {
		return false
	}
	// if one of the children is different, then the two GroupExpressions are different.
	for i, one := range e.Children {
		if !one.Equals(e2.Children[i]) {
			return false
		}
	}
	return true
}

// NewGroupExpression creates a new GroupExpression with the given logical plan and children.
func NewGroupExpression(lp base.LogicalPlan, children []*Group) *GroupExpression {
	return &GroupExpression{
		Group:       nil,
		Children:    children,
		LogicalPlan: lp,
		hash64:      0,
	}
}

// Init initializes the GroupExpression with the given group and hasher.
func (e *GroupExpression) Init(h base2.Hasher) {
	e.Hash64(h)
	e.hash64 = h.Sum64()
}
