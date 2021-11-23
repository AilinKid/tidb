// Copyright 2019 PingCAP, Inc.
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

package util

import (
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/ranger"
)

// AccessPath indicates the way we access a table: by using single index, or by using multiple indexes,
// or just by using table scan.
type AccessPath struct {
	Index          *model.IndexInfo
	FullIdxCols    []*expression.Column
	FullIdxColLens []int
	IdxCols        []*expression.Column
	IdxColLens     []int
	// ConstCols indicates whether the column is constant under the given conditions for all index columns.
	ConstCols []bool
	Ranges    []*ranger.Range
	// CountAfterAccess is the row count after we apply range seek and before we use other filter to filter data.
	// For index merge path, CountAfterAccess is the row count after partial paths and before we apply table filters.
	CountAfterAccess float64
	// CountAfterIndex is the row count after we apply filters on index and before we apply the table filters.
	CountAfterIndex float64
	AccessConds     []expression.Expression
	EqCondCount     int
	EqOrInCondCount int
	IndexFilters    []expression.Expression
	TableFilters    []expression.Expression
	// PartialIndexPaths store all index access paths.
	// If there are extra filters, store them in TableFilters.
	PartialIndexPaths []*AccessPath

	StoreType kv.StoreType

	IsDNFCond bool

	// IsTiFlashGlobalRead indicates whether this path is a remote read path for tiflash
	IsTiFlashGlobalRead bool

	// IsIntHandlePath indicates whether this path is table path.
	IsIntHandlePath    bool
	IsCommonHandlePath bool
	// Forced means this path is generated by `use/force index()`.
	Forced bool
	// IsSingleScan indicates whether the path is a single index/table scan or table access after index scan.
	IsSingleScan bool
}

// IsTablePath returns true if it's IntHandlePath or CommonHandlePath.
func (path *AccessPath) IsTablePath() bool {
	return path.IsIntHandlePath || path.IsCommonHandlePath
}

// SplitCorColAccessCondFromFilters move the necessary filter in the form of index_col = corrlated_col to access conditions.
// The function consider the `idx_col_1 = const and index_col_2 = cor_col and index_col_3 = const` case.
// It enables more index columns to be considered. The range will be rebuilt in 'ResolveCorrelatedColumns'.
func (path *AccessPath) SplitCorColAccessCondFromFilters(ctx sessionctx.Context, eqOrInCount int) (access, remained []expression.Expression) {
	// The plan cache do not support subquery now. So we skip this function when
	// 'MaybeOverOptimized4PlanCache' function return true .
	if expression.MaybeOverOptimized4PlanCache(ctx, path.TableFilters) {
		return nil, path.TableFilters
	}
	access = make([]expression.Expression, len(path.IdxCols)-eqOrInCount)
	used := make([]bool, len(path.TableFilters))
	for i := eqOrInCount; i < len(path.IdxCols); i++ {
		matched := false
		for j, filter := range path.TableFilters {
			if used[j] || !isColEqCorColOrConstant(ctx, filter, path.IdxCols[i]) {
				continue
			}
			matched = true
			access[i-eqOrInCount] = filter
			if path.IdxColLens[i] == types.UnspecifiedLength {
				used[j] = true
			}
			break
		}
		if !matched {
			access = access[:i-eqOrInCount]
			break
		}
	}
	for i, ok := range used {
		if !ok {
			remained = append(remained, path.TableFilters[i]) // nozero
		}
	}
	return access, remained
}

// isColEqCorColOrConstant checks if the expression is a eq function that one side is constant or correlated column
// and another is column.
func isColEqCorColOrConstant(ctx sessionctx.Context, filter expression.Expression, col *expression.Column) bool {
	f, ok := filter.(*expression.ScalarFunction)
	if !ok || f.FuncName.L != ast.EQ {
		return false
	}
	_, collation := f.CharsetAndCollation()
	if c, ok := f.GetArgs()[0].(*expression.Column); ok {
		if c.RetType.EvalType() == types.ETString && !collate.CompatibleCollate(collation, c.RetType.Collate) {
			return false
		}
		if _, ok := f.GetArgs()[1].(*expression.Constant); ok {
			if col.Equal(nil, c) {
				return true
			}
		}
		if _, ok := f.GetArgs()[1].(*expression.CorrelatedColumn); ok {
			if col.Equal(nil, c) {
				return true
			}
		}
	}
	if c, ok := f.GetArgs()[1].(*expression.Column); ok {
		if c.RetType.EvalType() == types.ETString && !collate.CompatibleCollate(collation, c.RetType.Collate) {
			return false
		}
		if _, ok := f.GetArgs()[0].(*expression.Constant); ok {
			if col.Equal(nil, c) {
				return true
			}
		}
		if _, ok := f.GetArgs()[0].(*expression.CorrelatedColumn); ok {
			if col.Equal(nil, c) {
				return true
			}
		}
	}
	return false
}

// OnlyPointRange checks whether each range is a point(no interval range exists).
func (path *AccessPath) OnlyPointRange(sctx sessionctx.Context) bool {
	noIntervalRange := true
	if path.IsIntHandlePath {
		for _, ran := range path.Ranges {
			if !ran.IsPoint(sctx) {
				noIntervalRange = false
				break
			}
		}
		return noIntervalRange
	}
	haveNullVal := false
	for _, ran := range path.Ranges {
		// Not point or the not full matched.
		if !ran.IsPoint(sctx) || len(ran.HighVal) != len(path.Index.Columns) {
			noIntervalRange = false
			break
		}
		// Check whether there's null value.
		for i := 0; i < len(path.Index.Columns); i++ {
			if ran.HighVal[i].IsNull() {
				haveNullVal = true
				break
			}
		}
		if haveNullVal {
			break
		}
	}
	return noIntervalRange && !haveNullVal
}
