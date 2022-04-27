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

//go:build ignore
// +build ignore

package main

import (
	"bytes"
	"go/format"
	"log"
	"os"
	"path/filepath"
	"text/template"

	. "github.com/pingcap/tidb/expression/generator/helper"
)

var addOrSubTime = template.Must(template.New("").Parse(`
{{ if eq $.FuncName "AddTime" }}
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

// Code generated by go generate in expression/generator; DO NOT EDIT.

package expression

import (
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)
{{ end }}
{{ define "SetNull" }}{{if .Output.Fixed}}result.SetNull(i, true){{else}}result.AppendNull(){{end}} // fixed: {{.Output.Fixed }}{{ end }}
{{ define "ConvertStringToDuration" }}
		{{ if and (ne .SigName "builtinAddStringAndStringSig") (ne .SigName "builtinSubStringAndStringSig") }}
		if !isDuration(arg1) {
			{{ template "SetNull" . }}
			continue
		}{{ end }}
		sc := b.ctx.GetSessionVars().StmtCtx
		arg1Duration, err := types.ParseDuration(sc, arg1, {{if eq .Output.TypeName "String"}}getFsp4TimeAddSub{{else}}types.GetFsp{{end}}(arg1))
		if err != nil {
			if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
				sc.AppendWarning(err)
				{{ template "SetNull" . }}
				continue
			}
			return err
		}
{{ end }}

{{ range .Sigs }}
{{ if .AllNull}}
func (b *{{.SigName}}) vecEval{{ .Output.TypeName }}(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	{{ if .Output.Fixed }}
	result.Resize{{ .Output.TypeNameInColumn }}(n, true)
	{{ else }}
	result.Reserve{{ .Output.TypeNameInColumn }}(n)
	for i := 0; i < n; i++ { result.AppendNull() }
	{{ end }}
	return nil
}
{{ else }}
func (b *{{.SigName}}) vecEval{{ .Output.TypeName }}(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
{{ $reuse := (and (eq .TypeA.TypeName .Output.TypeName) .TypeA.Fixed) }}
{{ if $reuse }}
	if err := b.args[0].VecEval{{ .TypeA.TypeName }}(b.ctx, input, result); err != nil {
		return err
	}
	buf0 := result
{{ else }}
	buf0, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEval{{ .TypeA.TypeName }}(b.ctx, input, buf0); err != nil {
		return err
	}
{{ end }}

{{ if or (eq .SigName "builtinAddStringAndStringSig") (eq .SigName "builtinSubStringAndStringSig") }}
	arg1Type := b.args[1].GetType()
	if mysql.HasBinaryFlag(arg1Type.GetFlag()) {
		result.Reserve{{ .Output.TypeNameInColumn }}(n)
		for i := 0; i < n; i++ {
			result.AppendNull()
		}
		return nil
	}
{{ end }}

	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEval{{ .TypeB.TypeName }}(b.ctx, input, buf1); err != nil {
		return err
	}

{{ if $reuse }}
	result.MergeNulls(buf1)
{{ else if .Output.Fixed}}
	result.Resize{{ .Output.TypeNameInColumn }}(n, false)
	result.MergeNulls(buf0, buf1)
{{ else }}
	result.Reserve{{ .Output.TypeNameInColumn}}(n)
{{ end }}

{{ if .TypeA.Fixed }}
	arg0s := buf0.{{.TypeA.TypeNameInColumn}}s()
{{ end }}
{{ if .TypeB.Fixed }}
	arg1s := buf1.{{.TypeB.TypeNameInColumn}}s()
{{ end }}
{{ if .Output.Fixed }}
	resultSlice := result.{{.Output.TypeNameInColumn}}s()
{{ end }}
	for i := 0; i < n; i++ {
		{{ if .Output.Fixed }}
		if result.IsNull(i) {
			continue
		}
		{{ else }}
		if buf0.IsNull(i) || buf1.IsNull(i) {
			result.AppendNull()
			continue
		}
		{{ end }}

		// get arg0 & arg1
		{{ if .TypeA.Fixed }}
		arg0 := arg0s[i]
		{{ else }}
		arg0 := buf0.Get{{ .TypeA.TypeNameInColumn }}(i)
		{{ end }}
		{{ if .TypeB.Fixed }}
		arg1 := arg1s[i]
		{{ else }}
		arg1 := buf1.Get{{ .TypeB.TypeNameInColumn }}(i)
		{{ end }}

		// calculate
	{{ if or (eq .SigName "builtinAddDatetimeAndDurationSig") (eq .SigName "builtinSubDatetimeAndDurationSig") }}
		{{ if eq $.FuncName "AddTime" }}
		output, err := arg0.Add(b.ctx.GetSessionVars().StmtCtx, types.Duration{Duration: arg1, Fsp: -1})
		{{ else }}
		sc := b.ctx.GetSessionVars().StmtCtx
		arg1Duration := types.Duration{Duration: arg1, Fsp: -1}
		output, err := arg0.Add(sc, arg1Duration.Neg())
		{{ end }}
		if err != nil {
			return err
		}

	{{ else if or (eq .SigName "builtinAddDatetimeAndStringSig") (eq .SigName "builtinSubDatetimeAndStringSig") }}
		{{ if eq $.FuncName "AddTime" }}
		{{ template "ConvertStringToDuration" . }}
		output, err := arg0.Add(sc, arg1Duration)
		{{ else }}
		if !isDuration(arg1) {
			result.SetNull(i, true) // fixed: true
			continue
		}
		sc := b.ctx.GetSessionVars().StmtCtx
		arg1Duration, err := types.ParseDuration(sc, arg1, types.GetFsp(arg1))
		if err != nil {
			if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
				sc.AppendWarning(err)
				result.SetNull(i, true) // fixed: true
				continue
			}
			return err
		}
		output, err := arg0.Add(sc, arg1Duration.Neg())
		{{ end }}
		if err != nil {
			return err
		}
	{{ else if or (eq .SigName "builtinAddDurationAndDurationSig") (eq .SigName "builtinSubDurationAndDurationSig") }}
		{{ if eq $.FuncName "AddTime" }}
		output, err := types.AddDuration(arg0, arg1)
		if err != nil {
			return err
		}
		{{ else }}
		output, err := types.SubDuration(arg0, arg1)
		if err != nil {
			return err
		}
		{{ end }}
	{{ else if or (eq .SigName "builtinAddDurationAndStringSig") (eq .SigName "builtinSubDurationAndStringSig") }}
		{{ template "ConvertStringToDuration" . }}
		{{ if eq $.FuncName "AddTime" }}
		output, err := types.AddDuration(arg0, arg1Duration.Duration)
		if err != nil {
			return err
		}
		{{ else }}
		output, err := types.SubDuration(arg0, arg1Duration.Duration)
		if err != nil {
			return err
		}
		{{ end }}
	{{ else if or (eq .SigName "builtinAddStringAndDurationSig") (eq .SigName "builtinSubStringAndDurationSig") }}
		sc := b.ctx.GetSessionVars().StmtCtx
		fsp1 := b.args[1].GetType().GetDecimal()
		arg1Duration := types.Duration{Duration: arg1, Fsp: fsp1}
		var output string
		var isNull bool
		if isDuration(arg0) {
			{{ if eq $.FuncName "AddTime" }}
			output, err = strDurationAddDuration(sc, arg0, arg1Duration)
			{{ else }}
			output, err = strDurationSubDuration(sc, arg0, arg1Duration)
			{{ end }}
			if err != nil {
				if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
					sc.AppendWarning(err)
					{{ template "SetNull" . }}
					continue
				}
				return err
			}
		} else {
			{{ if eq $.FuncName "AddTime" }}
			output, isNull, err = strDatetimeAddDuration(sc, arg0, arg1Duration)
			{{ else }}
			output, isNull, err = strDatetimeSubDuration(sc, arg0, arg1Duration)
			{{ end }}
			if err != nil {
				return err
			}
			if isNull {
				sc.AppendWarning(err)
				{{ template "SetNull" . }}
				continue
			}
		}
	{{ else if or (eq .SigName "builtinAddStringAndStringSig") (eq .SigName "builtinSubStringAndStringSig") }}
		{{ template "ConvertStringToDuration" . }}
		var output string
		var isNull bool
		if isDuration(arg0) {
			{{ if eq $.FuncName "AddTime" }}
			output, err = strDurationAddDuration(sc, arg0, arg1Duration)
			{{ else }}
			output, err = strDurationSubDuration(sc, arg0, arg1Duration)
			{{ end }}
			if err != nil {
				if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
					sc.AppendWarning(err)
					{{ template "SetNull" . }}
					continue
				}
				return err
			}
		} else {
			{{ if eq $.FuncName "AddTime" }}
			output, isNull, err = strDatetimeAddDuration(sc, arg0, arg1Duration)
			{{ else }}
			output, isNull, err = strDatetimeSubDuration(sc, arg0, arg1Duration)
			{{ end }}
			if err != nil {
				return err
			}
			if isNull {
				sc.AppendWarning(err)
				{{ template "SetNull" . }}
				continue
			}
		}
	{{ else if or (eq .SigName "builtinAddDateAndDurationSig") (eq .SigName "builtinSubDateAndDurationSig") }}
		fsp0 := b.args[0].GetType().GetDecimal()
		fsp1 := b.args[1].GetType().GetDecimal()
		arg1Duration := types.Duration{Duration: arg1, Fsp: fsp1}
		{{ if eq $.FuncName "AddTime" }}
		sum, err := types.Duration{Duration: arg0, Fsp: fsp0}.Add(arg1Duration)
		{{ else }}
		sum, err := types.Duration{Duration: arg0, Fsp: fsp0}.Sub(arg1Duration)
		{{ end }}
		if err != nil {
			return err
		}
		output := sum.String()
	{{ else if or (eq .SigName "builtinAddDateAndStringSig") (eq .SigName "builtinSubDateAndStringSig") }}
		{{ template "ConvertStringToDuration" . }}
		fsp0 := b.args[0].GetType().GetDecimal()
		{{ if eq $.FuncName "AddTime" }}
		sum, err := types.Duration{Duration: arg0, Fsp: fsp0}.Add(arg1Duration)
		{{ else }}
		sum, err := types.Duration{Duration: arg0, Fsp: fsp0}.Sub(arg1Duration)
		{{ end }}
		if err != nil {
			return err
		}
		output := sum.String()
	{{ end }}

		// commit result
	{{ if .Output.Fixed }}
		resultSlice[i] = output
	{{ else }}
		result.Append{{ .Output.TypeNameInColumn }}(output)
	{{ end }}
	}
	return nil
}
{{ end }}{{/* if .AllNull */}}

func (b *{{.SigName}}) vectorized() bool {
	return true
}
{{ end }}{{/* range */}}
`))

var timeDiff = template.Must(template.New("").Parse(`
{{ define "BufAllocator0" }}
	buf0, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
{{ end }}
{{ define "BufAllocator1" }}
	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
{{ end }}
{{ define "ArgsVecEval" }}
	if err := b.args[0].VecEval{{ .TypeA.TypeName }}(b.ctx, input, buf0); err != nil {
		return err
	}
	if err := b.args[1].VecEval{{ .TypeB.TypeName }}(b.ctx, input, buf1); err != nil {
		return err
	}
{{ end }}

{{ range . }}
{{ $AIsString := (eq .TypeA.TypeName "String") }}
{{ $BIsString := (eq .TypeB.TypeName "String") }}
{{ $AIsTime := (eq .TypeA.TypeName "Time") }}
{{ $BIsTime := (eq .TypeB.TypeName "Time") }}
{{ $AIsDuration := (eq .TypeA.TypeName "Duration") }}
{{ $BIsDuration := (eq .TypeB.TypeName "Duration") }}
{{ $MaybeDuration := (or (or $AIsDuration $BIsDuration) (and $AIsString $AIsString)) }}
{{ $reuseA := (eq .TypeA.TypeName "Duration") }}
{{ $reuseB := (eq .TypeB.TypeName "Duration") }}
{{ $reuse  := (or $reuseA $reuseB ) }}
{{ $noNull := (ne .SigName "builtinNullTimeDiffSig") }}
func (b *{{.SigName}}) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	{{- if not $noNull }}
	result.ResizeGoDuration(n, true)
	{{- else }}
		result.ResizeGoDuration(n, false)
		r64s := result.GoDurations()
		{{- if $reuseA }}
			buf0 := result
			{{- template "BufAllocator1" . }}
			{{- template "ArgsVecEval" . }}
			result.MergeNulls(buf1)
		{{- else if $reuseB }}
			buf1 := result
			{{- template "BufAllocator0" . }}
			{{- template "ArgsVecEval" . }}
			result.MergeNulls(buf0)
		{{- else }}
			{{- template "BufAllocator0" . }}
			{{- template "BufAllocator1" . }}
			{{- template "ArgsVecEval" . }}
			result.MergeNulls(buf0, buf1)
		{{- end }}
		{{- if .TypeA.Fixed }}
			arg0 := buf0.{{.TypeA.TypeNameInColumn}}s()
		{{- end }}
		{{- if .TypeB.Fixed }}
			arg1 := buf1.{{.TypeB.TypeNameInColumn}}s()
		{{- end }}

		{{- if (or $AIsDuration $BIsDuration) }}
			var (
				lhs    types.Duration
				rhs    types.Duration
			)
		{{- end }}
		{{- if or (or $AIsString $BIsString) (and $AIsTime $BIsTime) }}
			stmtCtx := b.ctx.GetSessionVars().StmtCtx
		{{- end }}
	for i:=0; i<n ; i++{
		if result.IsNull(i) {
			continue
		}

		{{- if $AIsString }}
			{{ if $BIsDuration }} lhsDur, _, lhsIsDuration,
			{{- else if $BIsTime }} _, lhsTime, lhsIsDuration,
			{{- else if $BIsString }} lhsDur, lhsTime, lhsIsDuration,
			{{- end }}  err := convertStringToDuration(stmtCtx, buf0.GetString(i), b.tp.GetDecimal())
			if err != nil  {
				return err
			}
			{{- if $BIsDuration }}
			if !lhsIsDuration {
				result.SetNull(i, true)
				continue
			}
			lhs = lhsDur
			{{- else if $BIsTime }}
			if lhsIsDuration {
				result.SetNull(i, true)
				continue
			}
			{{- end }}
		{{- else if $AIsTime }}
			lhsTime := arg0[i]
		{{- else }}
			lhs.Duration = arg0[i]
		{{- end }}

		{{- if $BIsString }}
			{{ if $AIsDuration }} rhsDur, _, rhsIsDuration,
			{{- else if $AIsTime }}_, rhsTime, rhsIsDuration,
			{{- else if $AIsString }} rhsDur, rhsTime, rhsIsDuration,
			{{- end}}  err := convertStringToDuration(stmtCtx, buf1.GetString(i), b.tp.GetDecimal())
			if err != nil  {
				return err
			}
			{{- if $AIsDuration }}
			if !rhsIsDuration {
				result.SetNull(i, true)
				continue
			}
			rhs = rhsDur
			{{- else if $AIsTime }}
			if rhsIsDuration {
				result.SetNull(i, true)
				continue
			}
			{{- end }}
		{{- else if $BIsTime }}
			rhsTime := arg1[i]
		{{- else }}
			rhs.Duration = arg1[i]
		{{- end }}

		{{- if and $AIsString $BIsString }}
			if lhsIsDuration != rhsIsDuration {
				result.SetNull(i, true)
				continue
			}
			var (
				d types.Duration
				isNull bool
			)
			if lhsIsDuration {
				d, isNull, err = calculateDurationTimeDiff(b.ctx, lhsDur, rhsDur)
			} else {
				d, isNull, err = calculateTimeDiff(stmtCtx, lhsTime, rhsTime)
			}
		{{- else if or $AIsDuration $BIsDuration }}
			d, isNull, err := calculateDurationTimeDiff(b.ctx, lhs, rhs)
		{{- else if or $AIsTime $BIsTime }}
			d, isNull, err := calculateTimeDiff(stmtCtx, lhsTime, rhsTime)
		{{- end }}
		if err != nil {
			return err
		}
		if isNull {
			result.SetNull(i, true)
			continue
		}
		r64s[i] = d.Duration
	}
	{{- end }} {{/* if $noNull */}}
	return nil
}

func (b *{{.SigName}}) vectorized() bool {
	return true
}
{{ end }}{{/* range */}}
`))

var addOrSubDate = template.Must(template.New("").Parse(`
{{ range .Sigs }}
{{- if eq .TypeA.TypeName "String"}}
func (b *{{.SigName}}) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
n := input.NumRows()
	unit, isNull, err := b.args[2].EvalString(b.ctx, chunk.Row{})
	if err != nil {
		return err
	}
	if isNull {
		result.ReserveString(n)
        result.SetNulls(0, n, true)
		return nil
	}

	intervalBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(intervalBuf)
	if err := b.vecGetIntervalFrom{{.TypeB.ETName}}(&b.baseBuiltinFunc, input, unit, intervalBuf); err != nil {
		return err
	}

	dateBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(dateBuf)
	if err := b.vecGetDateFromString(&b.baseBuiltinFunc, input, unit, dateBuf); err != nil {
			return err
		}

	isClockUnit := types.IsClockUnit(unit)

	result.ReserveString(n)

	dateBuf.MergeNulls(intervalBuf)
	for i := 0; i < n; i++ {
		if dateBuf.IsNull(i) {
			result.AppendNull()
			continue
		}
        {{- if eq $.FuncName "AddDate" }}
        resDate, isNull, err := b.add(b.ctx, dateBuf.Times()[i], intervalBuf.GetString(i), unit)
        {{- else }}
        resDate, isNull, err := b.sub(b.ctx, dateBuf.Times()[i], intervalBuf.GetString(i), unit)
        {{- end }}
		if err != nil {
			return err
		}
		if isNull {
			result.AppendNull()
		} else {
			dateTp := mysql.TypeDate
			if dateBuf.Times()[i].Type() == mysql.TypeDatetime || isClockUnit {
				dateTp = mysql.TypeDatetime
			}
			resDate.SetType(dateTp)
			result.AppendString(resDate.String())
		}
	}
	return nil
}
{{- else }}
{{- if eq .TypeA.TypeName "Duration" }}
func (b *{{.SigName}}) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
{{- else }}
func (b *{{.SigName}}) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
{{- end }}
	n := input.NumRows()
	unit, isNull, err := b.args[2].EvalString(b.ctx, chunk.Row{})
	if err != nil {
		return err
	}
	if isNull {
		{{- if eq .TypeA.TypeName "Duration" }}
			result.ResizeGoDuration(n, true)
		{{- else }}
			result.ResizeTime(n, true)
		{{- end }}
		return nil
	}

	intervalBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(intervalBuf)
	if err := b.vecGetIntervalFrom{{.TypeB.ETName}}(&b.baseBuiltinFunc, input, unit, intervalBuf); err != nil {
		return err
	}

	{{ if eq .TypeA.TypeName "Duration" }}
		result.ResizeGoDuration(n, false)
		if err := b.args[0].VecEvalDuration(b.ctx, input, result); err != nil {
			return err
		}

		result.MergeNulls(intervalBuf)
		resDurations := result.GoDurations()
		iterDuration := types.Duration{Fsp: types.MaxFsp}
	{{ else }}
		if err := b.vecGetDateFrom{{.TypeA.ETName}}(&b.baseBuiltinFunc, input, unit, result); err != nil {
			return err
		}

		result.MergeNulls(intervalBuf)
		resDates := result.Times()
	{{ end -}}

	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}

		{{- if eq .TypeA.TypeName "Duration" }}
			iterDuration.Duration = resDurations[i]
			{{- if eq $.FuncName "AddDate" }}
				resDuration, isNull, err := b.addDuration(b.ctx, iterDuration, intervalBuf.GetString(i), unit)
			{{- else }}
				resDuration, isNull, err := b.subDuration(b.ctx, iterDuration, intervalBuf.GetString(i), unit)
			{{- end }}
		{{- else }}
			{{- if eq $.FuncName "AddDate" }}
				resDate, isNull, err := b.add(b.ctx, resDates[i], intervalBuf.GetString(i), unit)
			{{- else }}
				resDate, isNull, err := b.sub(b.ctx, resDates[i], intervalBuf.GetString(i), unit)
			{{- end }}
		{{- end }}
		if err != nil {
			return err
		}
		if isNull {
			result.SetNull(i, true)
		} else {
			{{- if eq .TypeA.TypeName "Duration" }}
				resDurations[i] = resDuration.Duration
			{{- else }}
				resDates[i] = resDate
			{{- end }}
		}
	}
	return nil
}
{{- end }}

func (b *{{.SigName}}) vectorized() bool {
	return true
}
{{ end }}{{/* range */}}
`))

var testFileFuncs = template.FuncMap{
	"getIntervalUnitList": func() []string {
		return []string{
			"MICROSECOND",
			"SECOND",
			"MINUTE",
			"HOUR",
			"DAY",
			"WEEK",
			"MONTH",
			"QUARTER",
			"YEAR",
			"SECOND_MICROSECOND",
			"MINUTE_MICROSECOND",
			"MINUTE_SECOND",
			"HOUR_MICROSECOND",
			"HOUR_SECOND",
			"HOUR_MINUTE",
			"DAY_MICROSECOND",
			"DAY_SECOND",
			"DAY_MINUTE",
			"DAY_HOUR",
			"YEAR_MONTH",
		}
	},
}

var testFile = template.Must(template.New("").Funcs(testFileFuncs).Parse(`
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

// Code generated by go generate in expression/generator; DO NOT EDIT.

package expression

import (
	"math"
	"testing"

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
)

type gener struct {
	defaultGener
}

func (g gener) gen() interface{} {
	result := g.defaultGener.gen()
	if _, ok := result.(string); ok {
		dg := newDefaultGener(0, types.ETDuration)
		d := dg.gen().(types.Duration)
		if d.Duration%2 == 0 {
			d.Fsp = 0
		} else {
			d.Fsp = 1
		}
		result = d.String()
	}
	return result
}

{{ define "addOrSubDateCases" }}
	{{- $unitList := getIntervalUnitList -}}
	{{- range $sig := .Sigs }}
		// {{ $sig.SigName }}
		{{- range $unit := $unitList }}
			{
				retEvalType: types.ET{{ $sig.Output.ETName }},
				childrenTypes: []types.EvalType{types.ET{{ $sig.TypeA.ETName }}, types.ET{{ $sig.TypeB.ETName }}, types.ETString},
				geners: []dataGenerator{
					{{- if eq $sig.TypeA.ETName "String"}}
						&dateStrGener{NullRation: 0.2, randGen: newDefaultRandGen()},
					{{- else if eq $sig.TypeA.ETName "Int"}}
						&dateTimeIntGener{dateTimeGener: dateTimeGener{randGen: newDefaultRandGen()}, nullRation: 0.2},
					{{- else }}
						newDefaultGener(0.2, types.ET{{$sig.TypeA.ETName}}),
					{{- end }}

					{{- if eq $sig.TypeB.ETName "String" }}
						&numStrGener{rangeInt64Gener{math.MinInt32 + 1, math.MaxInt32, newDefaultRandGen()}},
					{{- else }}
						newDefaultGener(0.2, types.ET{{$sig.TypeB.ETName}}),
					{{- end }}
				},
				constants: []*Constant{nil, nil, {Value: types.NewStringDatum("{{$unit}}"), RetType: types.NewFieldType(mysql.TypeString)}},
				chunkSize: 128,
			},
		{{- end }}
	{{- end }}
{{ end }}

{{ define "addOrSubTimeCases" }}
	{{- range $sig := .Sigs }}
		// {{ $sig.SigName }}
			{
				retEvalType: types.ET{{ .Output.ETName }},
				{{- if eq .TestTypeA "" }}
				childrenTypes: []types.EvalType{types.ET{{ .TypeA.ETName }}, types.ET{{ .TypeB.ETName }}},
				{{- else }}
				childrenTypes: []types.EvalType{types.ET{{ .TestTypeA }}, types.ET{{ .TestTypeB }}},
				{{- end }}
				{{- if ne .FieldTypeA "" }}
				childrenFieldTypes: []*types.FieldType{types.NewFieldType(mysql.Type{{.FieldTypeA}}), types.NewFieldType(mysql.Type{{.FieldTypeB}})},
				{{- end }}
				geners: []dataGenerator{
					{{- if eq .TestTypeA "" }}
					gener{*newDefaultGener(0.2, types.ET{{.TypeA.ETName}})},
					gener{*newDefaultGener(0.2, types.ET{{.TypeB.ETName}})},
					{{- else }}
					gener{*newDefaultGener(0.2, types.ET{{ .TestTypeA }})},
					gener{*newDefaultGener(0.2, types.ET{{ .TestTypeB }})},
					{{- end }}
				},
			},
	{{- end }}
{{ end }}

{{/* Add more test cases here if we have more functions in this file */}}
var vecBuiltin{{.Category}}GeneratedCases = map[string][]vecExprBenchCase{
{{- range .Functions }}
	{{- if eq .FuncName "AddTime" }}
		ast.AddTime: {
			{{- template "addOrSubTimeCases" . -}}
		},
	{{ end }}
	{{- if eq .FuncName "SubTime" }}
		ast.SubTime: {
			{{- template "addOrSubTimeCases" . -}}
		},
	{{ end }}
	{{- if eq .FuncName "TimeDiff" }}
		ast.TimeDiff: {
			// builtinNullTimeDiffSig
			{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETDuration, types.ETDatetime}},
			{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETDuration, types.ETTimestamp}},
			{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETDatetime, types.ETDuration}},
			{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETTimestamp, types.ETDuration}},
			// builtinDurationDurationTimeDiffSig
			{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETDuration, types.ETDuration}},
			// builtinDurationStringTimeDiffSig
			{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETDuration, types.ETString}, geners: []dataGenerator{nil, &dateTimeStrGener{Year: 2019, Month: 11, randGen: newDefaultRandGen()}}},
			{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETDuration, types.ETString}, geners: []dataGenerator{nil, &dateTimeStrGener{Year: 2019, Month: 10, randGen: newDefaultRandGen()}}},
			{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETDuration, types.ETString}, geners: []dataGenerator{nil, &dateTimeStrGener{Year: 2019, Month: 10, Fsp: 4, randGen: newDefaultRandGen()}}},
			// builtinTimeTimeTimeDiffSig
			{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETDatetime, types.ETDatetime}, geners: []dataGenerator{&dateTimeGener{Year: 2019, Month: 10, randGen: newDefaultRandGen()}, &dateTimeGener{Year: 2019, Month: 10, randGen: newDefaultRandGen()}}},
			{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETDatetime, types.ETTimestamp}, geners: []dataGenerator{&dateTimeGener{Year: 2019, Month: 10, randGen: newDefaultRandGen()}, &dateTimeGener{Year: 2019, Month: 10, randGen: newDefaultRandGen()}}},
			{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETTimestamp, types.ETTimestamp}, geners: []dataGenerator{&dateTimeGener{Year: 2019, Month: 10, randGen: newDefaultRandGen()}, &dateTimeGener{Year: 2019, Month: 10, randGen: newDefaultRandGen()}}},
			{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETTimestamp, types.ETDatetime}, geners: []dataGenerator{&dateTimeGener{Year: 2019, Month: 10, randGen: newDefaultRandGen()}, &dateTimeGener{Year: 2019, Month: 10, randGen: newDefaultRandGen()}}},
			// builtinTimeStringTimeDiffSig
			{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETDatetime, types.ETString}, geners: []dataGenerator{&dateTimeGener{Year: 2019, Month: 10, randGen: newDefaultRandGen()}, &dateTimeStrGener{Year: 2019, Month: 10, randGen: newDefaultRandGen()}}},
			{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETTimestamp, types.ETString}, geners: []dataGenerator{&dateTimeGener{Year: 2019, Month: 10, randGen: newDefaultRandGen()}, &dateTimeStrGener{Year: 2019, Month: 10, randGen: newDefaultRandGen()}}},
			// builtinStringDurationTimeDiffSig
			{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETString, types.ETDuration}, geners: []dataGenerator{&dateTimeStrGener{Year: 2019, Month: 10, randGen: newDefaultRandGen()}, nil}},
			// builtinStringTimeTimeDiffSig
			{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETString, types.ETDatetime}, geners: []dataGenerator{&dateTimeStrGener{Year: 2019, Month: 10, randGen: newDefaultRandGen()}, &dateTimeGener{Year: 2019, Month: 10, randGen: newDefaultRandGen()}}},
			{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETString, types.ETTimestamp}, geners: []dataGenerator{&dateTimeStrGener{Year: 2019, Month: 10, randGen: newDefaultRandGen()}, &dateTimeGener{Year: 2019, Month: 10, randGen: newDefaultRandGen()}}},
			// builtinStringStringTimeDiffSig
			{retEvalType: types.ETDuration, childrenTypes: []types.EvalType{types.ETString, types.ETString}, geners: []dataGenerator{&dateTimeStrGener{Year: 2019, Month: 10, randGen: newDefaultRandGen()}, &dateTimeStrGener{Year: 2019, Month: 10, randGen: newDefaultRandGen()}}},
		},
	{{ end }}
	{{- if eq .FuncName "AddDate" }}
		ast.AddDate: {
			{{- template "addOrSubDateCases" . -}}
		},
	{{ end }}
	{{- if eq .FuncName "SubDate" }}
		ast.SubDate: {
			{{- template "addOrSubDateCases" . -}}
		},
	{{ end }}
{{ end }}
}

func TestVectorizedBuiltin{{.Category}}EvalOneVecGenerated(t *testing.T) {
	testVectorizedEvalOneVec(t, vecBuiltin{{.Category}}GeneratedCases)
}

func TestVectorizedBuiltin{{.Category}}FuncGenerated(t *testing.T) {
	testVectorizedBuiltinFunc(t, vecBuiltin{{.Category}}GeneratedCases)
}

func BenchmarkVectorizedBuiltin{{.Category}}EvalOneVecGenerated(b *testing.B) {
	benchmarkVectorizedEvalOneVec(b, vecBuiltin{{.Category}}GeneratedCases)
}

func BenchmarkVectorizedBuiltin{{.Category}}FuncGenerated(b *testing.B) {
	benchmarkVectorizedBuiltinFunc(b, vecBuiltin{{.Category}}GeneratedCases)
}
`))

var addTimeSigsTmpl = []sig{
	{SigName: "builtinAddDatetimeAndDurationSig", TypeA: TypeDatetime, TypeB: TypeDuration, Output: TypeDatetime},
	{SigName: "builtinAddDatetimeAndStringSig", TypeA: TypeDatetime, TypeB: TypeString, Output: TypeDatetime},
	{SigName: "builtinAddDurationAndDurationSig", TypeA: TypeDuration, TypeB: TypeDuration, Output: TypeDuration},
	{SigName: "builtinAddDurationAndStringSig", TypeA: TypeDuration, TypeB: TypeString, Output: TypeDuration},
	{SigName: "builtinAddStringAndDurationSig", TypeA: TypeString, TypeB: TypeDuration, Output: TypeString},
	{SigName: "builtinAddStringAndStringSig", TypeA: TypeString, TypeB: TypeString, Output: TypeString},
	{SigName: "builtinAddDateAndDurationSig", TypeA: TypeDuration, TypeB: TypeDuration, Output: TypeString, FieldTypeA: "Date", FieldTypeB: "Duration", TestTypeA: "Datetime", TestTypeB: "Duration"},
	{SigName: "builtinAddDateAndStringSig", TypeA: TypeDuration, TypeB: TypeString, Output: TypeString, FieldTypeA: "Date", FieldTypeB: "String", TestTypeA: "Datetime", TestTypeB: "String"},

	{SigName: "builtinAddTimeDateTimeNullSig", TypeA: TypeDatetime, TypeB: TypeDatetime, Output: TypeDatetime, AllNull: true},
	{SigName: "builtinAddTimeStringNullSig", TypeA: TypeDatetime, TypeB: TypeDatetime, Output: TypeString, AllNull: true, FieldTypeA: "Date", FieldTypeB: "Datetime"},
	{SigName: "builtinAddTimeDurationNullSig", TypeA: TypeDuration, TypeB: TypeDatetime, Output: TypeDuration, AllNull: true},
}

var subTimeSigsTmpl = []sig{
	{SigName: "builtinSubDatetimeAndDurationSig", TypeA: TypeDatetime, TypeB: TypeDuration, Output: TypeDatetime},
	{SigName: "builtinSubDatetimeAndStringSig", TypeA: TypeDatetime, TypeB: TypeString, Output: TypeDatetime},
	{SigName: "builtinSubDurationAndDurationSig", TypeA: TypeDuration, TypeB: TypeDuration, Output: TypeDuration},
	{SigName: "builtinSubDurationAndStringSig", TypeA: TypeDuration, TypeB: TypeString, Output: TypeDuration},
	{SigName: "builtinSubStringAndDurationSig", TypeA: TypeString, TypeB: TypeDuration, Output: TypeString},
	{SigName: "builtinSubStringAndStringSig", TypeA: TypeString, TypeB: TypeString, Output: TypeString},
	{SigName: "builtinSubDateAndDurationSig", TypeA: TypeDuration, TypeB: TypeDuration, Output: TypeString, FieldTypeA: "Date", FieldTypeB: "Duration", TestTypeA: "Datetime", TestTypeB: "Duration"},
	{SigName: "builtinSubDateAndStringSig", TypeA: TypeDuration, TypeB: TypeString, Output: TypeString, FieldTypeA: "Date", FieldTypeB: "String", TestTypeA: "Datetime", TestTypeB: "String"},

	{SigName: "builtinSubTimeDateTimeNullSig", TypeA: TypeDatetime, TypeB: TypeDatetime, Output: TypeDatetime, AllNull: true},
	{SigName: "builtinSubTimeStringNullSig", TypeA: TypeDatetime, TypeB: TypeDatetime, Output: TypeString, AllNull: true, FieldTypeA: "Date", FieldTypeB: "Datetime"},
	{SigName: "builtinSubTimeDurationNullSig", TypeA: TypeDuration, TypeB: TypeDatetime, Output: TypeDuration, AllNull: true},
}

var timeDiffSigsTmpl = []sig{
	{SigName: "builtinNullTimeDiffSig", Output: TypeDuration},
	{SigName: "builtinTimeStringTimeDiffSig", TypeA: TypeDatetime, TypeB: TypeString, Output: TypeDuration},
	{SigName: "builtinDurationStringTimeDiffSig", TypeA: TypeDuration, TypeB: TypeString, Output: TypeDuration},
	{SigName: "builtinDurationDurationTimeDiffSig", TypeA: TypeDuration, TypeB: TypeDuration, Output: TypeDuration},
	{SigName: "builtinStringTimeTimeDiffSig", TypeA: TypeString, TypeB: TypeDatetime, Output: TypeDuration},
	{SigName: "builtinStringDurationTimeDiffSig", TypeA: TypeString, TypeB: TypeDuration, Output: TypeDuration},
	{SigName: "builtinStringStringTimeDiffSig", TypeA: TypeString, TypeB: TypeString, Output: TypeDuration},
	{SigName: "builtinTimeTimeTimeDiffSig", TypeA: TypeDatetime, TypeB: TypeDatetime, Output: TypeDuration},
}

var addDataSigsTmpl = []sig{
	{SigName: "builtinAddDateStringStringSig", TypeA: TypeString, TypeB: TypeString, Output: TypeString},
	{SigName: "builtinAddDateStringIntSig", TypeA: TypeString, TypeB: TypeInt, Output: TypeString},
	{SigName: "builtinAddDateStringRealSig", TypeA: TypeString, TypeB: TypeReal, Output: TypeString},
	{SigName: "builtinAddDateStringDecimalSig", TypeA: TypeString, TypeB: TypeDecimal, Output: TypeString},
	{SigName: "builtinAddDateIntStringSig", TypeA: TypeInt, TypeB: TypeString, Output: TypeDatetime},
	{SigName: "builtinAddDateIntIntSig", TypeA: TypeInt, TypeB: TypeInt, Output: TypeDatetime},
	{SigName: "builtinAddDateIntRealSig", TypeA: TypeInt, TypeB: TypeReal, Output: TypeDatetime},
	{SigName: "builtinAddDateIntDecimalSig", TypeA: TypeInt, TypeB: TypeDecimal, Output: TypeDatetime},
	{SigName: "builtinAddDateDatetimeStringSig", TypeA: TypeDatetime, TypeB: TypeString, Output: TypeDatetime},
	{SigName: "builtinAddDateDatetimeIntSig", TypeA: TypeDatetime, TypeB: TypeInt, Output: TypeDatetime},
	{SigName: "builtinAddDateDatetimeRealSig", TypeA: TypeDatetime, TypeB: TypeReal, Output: TypeDatetime},
	{SigName: "builtinAddDateDatetimeDecimalSig", TypeA: TypeDatetime, TypeB: TypeDecimal, Output: TypeDatetime},
	{SigName: "builtinAddDateDurationStringSig", TypeA: TypeDuration, TypeB: TypeString, Output: TypeDuration},
	{SigName: "builtinAddDateDurationIntSig", TypeA: TypeDuration, TypeB: TypeInt, Output: TypeDuration},
	{SigName: "builtinAddDateDurationRealSig", TypeA: TypeDuration, TypeB: TypeReal, Output: TypeDuration},
	{SigName: "builtinAddDateDurationDecimalSig", TypeA: TypeDuration, TypeB: TypeDecimal, Output: TypeDuration},
}

var subDataSigsTmpl = []sig{
	{SigName: "builtinSubDateStringStringSig", TypeA: TypeString, TypeB: TypeString, Output: TypeString},
	{SigName: "builtinSubDateStringIntSig", TypeA: TypeString, TypeB: TypeInt, Output: TypeString},
	{SigName: "builtinSubDateStringRealSig", TypeA: TypeString, TypeB: TypeReal, Output: TypeString},
	{SigName: "builtinSubDateStringDecimalSig", TypeA: TypeString, TypeB: TypeDecimal, Output: TypeString},
	{SigName: "builtinSubDateIntStringSig", TypeA: TypeInt, TypeB: TypeString, Output: TypeDatetime},
	{SigName: "builtinSubDateIntIntSig", TypeA: TypeInt, TypeB: TypeInt, Output: TypeDatetime},
	{SigName: "builtinSubDateIntRealSig", TypeA: TypeInt, TypeB: TypeReal, Output: TypeDatetime},
	{SigName: "builtinSubDateIntDecimalSig", TypeA: TypeInt, TypeB: TypeDecimal, Output: TypeDatetime},
	{SigName: "builtinSubDateDatetimeStringSig", TypeA: TypeDatetime, TypeB: TypeString, Output: TypeDatetime},
	{SigName: "builtinSubDateDatetimeIntSig", TypeA: TypeDatetime, TypeB: TypeInt, Output: TypeDatetime},
	{SigName: "builtinSubDateDatetimeRealSig", TypeA: TypeDatetime, TypeB: TypeReal, Output: TypeDatetime},
	{SigName: "builtinSubDateDatetimeDecimalSig", TypeA: TypeDatetime, TypeB: TypeDecimal, Output: TypeDatetime},
	{SigName: "builtinSubDateDurationStringSig", TypeA: TypeDuration, TypeB: TypeString, Output: TypeDuration},
	{SigName: "builtinSubDateDurationIntSig", TypeA: TypeDuration, TypeB: TypeInt, Output: TypeDuration},
	{SigName: "builtinSubDateDurationRealSig", TypeA: TypeDuration, TypeB: TypeReal, Output: TypeDuration},
	{SigName: "builtinSubDateDurationDecimalSig", TypeA: TypeDuration, TypeB: TypeDecimal, Output: TypeDuration},
}

type sig struct {
	SigName                string
	TypeA, TypeB, Output   TypeContext
	FieldTypeA, FieldTypeB string // Optional
	TestTypeA, TestTypeB   string // Optional, specific Type for test in builtinAddDateAndDurationSig & builtinAddDateAndStringSig
	AllNull                bool
}

type function struct {
	FuncName string
	Sigs     []sig
}

var tmplVal = struct {
	Category  string
	Functions []function
}{
	Category: "Time",
	Functions: []function{
		{FuncName: "AddTime", Sigs: addTimeSigsTmpl},
		{FuncName: "SubTime", Sigs: subTimeSigsTmpl},
		{FuncName: "TimeDiff", Sigs: timeDiffSigsTmpl},
		{FuncName: "AddDate", Sigs: addDataSigsTmpl},
		{FuncName: "SubDate", Sigs: subDataSigsTmpl},
	},
}

func generateDotGo(fileName string) error {
	w := new(bytes.Buffer)
	err := addOrSubTime.Execute(w, function{FuncName: "AddTime", Sigs: addTimeSigsTmpl})
	if err != nil {
		return err
	}
	err = addOrSubTime.Execute(w, function{FuncName: "SubTime", Sigs: subTimeSigsTmpl})
	if err != nil {
		return err
	}
	err = timeDiff.Execute(w, timeDiffSigsTmpl)
	if err != nil {
		return err
	}
	err = addOrSubDate.Execute(w, function{FuncName: "AddDate", Sigs: addDataSigsTmpl})
	if err != nil {
		return err
	}
	err = addOrSubDate.Execute(w, function{FuncName: "SubDate", Sigs: subDataSigsTmpl})
	if err != nil {
		return err
	}
	data, err := format.Source(w.Bytes())
	if err != nil {
		log.Println("[Warn]", fileName+": gofmt failed", err)
		data = w.Bytes() // write original data for debugging
	}
	return os.WriteFile(fileName, data, 0644)
}

func generateTestDotGo(fileName string) error {
	w := new(bytes.Buffer)
	err := testFile.Execute(w, tmplVal)
	if err != nil {
		return err
	}
	data, err := format.Source(w.Bytes())
	if err != nil {
		log.Println("[Warn]", fileName+": gofmt failed", err)
		data = w.Bytes() // write original data for debugging
	}
	return os.WriteFile(fileName, data, 0644)
}

// generateOneFile generate one xxx.go file and the associated xxx_test.go file.
func generateOneFile(fileNamePrefix string) (err error) {

	err = generateDotGo(fileNamePrefix + ".go")
	if err != nil {
		return
	}
	err = generateTestDotGo(fileNamePrefix + "_test.go")
	return
}

func main() {
	var err error
	outputDir := "."
	err = generateOneFile(filepath.Join(outputDir, "builtin_time_vec_generated"))
	if err != nil {
		log.Fatalln("generateOneFile", err)
	}
}
