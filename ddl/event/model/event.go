// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package model

import (
	"math"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
)

// EventEnableType is used to indicates whether the event is enabled or not.
type EventEnableType uint8

const (
	// TypeNone is unknown type.
	TypeNone EventEnableType = iota
	// TypeDisabled means the event is disabled.
	TypeDisabled
	// TypeEnabled means the event is enabled.
	TypeEnabled
	// TypeSlaveSideDisabled means the event is enabled only on the master.
	TypeSlaveSideDisabled
)

func (t EventEnableType) String() string {
	switch t {
	//enum('ENABLED','DISABLED','SLAVESIDE_DISABLED')
	case TypeEnabled:
		return "ENABLED"
	case TypeDisabled:
		return "DISABLED"
	case TypeSlaveSideDisabled:
		return "SLAVESIDE_DISABLED"
	default:
		return "UNKNOWN"
	}
}

// FormEventEnableType form a EventEnableType from a string.
func FormEventEnableType(s string) EventEnableType {
	switch s {
	case "ENABLED":
		return TypeEnabled
	case "DISABLED":
		return TypeDisabled
	case "SLAVESIDE_DISABLED":
		return TypeSlaveSideDisabled
	default:
		return TypeNone

	}
}

// EventInfo describes what event is like.
// TODO: move this part to parser.
type EventInfo struct {
	EventID         int64
	EventName       model.CIStr
	EventSchemaID   int64
	EventSchemaName model.CIStr

	Definer *auth.UserIdentity
	// @@sql_mode when the event was created
	SQLMode mysql.SQLMode
	// @@time_zone when the event was created
	TimeZone  string
	BodyType  string
	EventType string
	// the statement to execute
	Statement string
	// the statement to display in SHOW CREATE EVENT / SHOW EVENTS, with secret stuff wiped out
	SecureStmt string
	Enable     EventEnableType
	// ExecuteAt Ts in UTC
	ExecuteAt types.Time
	// start TS in UTC
	Starts types.Time
	// end TS in UTC
	Ends types.Time
	// repeat interval ("EVERY" clause)
	IntervalValue string
	IntervalUnit  ast.TimeUnitType

	Preserve bool
	// server ID
	Originator int64
	// UUID of the instance, empty string = any
	Instance string

	Charset   string
	Collation string
	Comment   string
	// Computed next execute time.
	NextExecuteAt types.Time
}

// ComputeNextExecuteTime compute the next execution time of this event.
func (e *EventInfo) ComputeNextExecuteUTCTime(sctx sessionctx.Context) error {
	if e.EventType == "ONE TIME" || !e.ExecuteAt.IsZero() {
		// For one time type event
		if e.NextExecuteAt.IsZero() {
			// The event hasn't been executed even once.
			e.NextExecuteAt = e.ExecuteAt
		} else {
			e.Enable = TypeDisabled
		}
	} else {
		if e.NextExecuteAt.IsZero() {
			e.NextExecuteAt = e.Starts
		} else {
			v, err := strconv.Atoi(e.IntervalValue)
			if err != nil {
				return errors.Trace(err)
			}
			d, err := e.IntervalUnit.Duration()
			if err != nil {
				return errors.Trace(err)
			}
			duration := time.Duration(v) * d
			next, err := e.NextExecuteAt.Add(sctx.GetSessionVars().StmtCtx, types.Duration{Duration: duration})
			if err != nil {
				return errors.Trace(err)
			}
			e.NextExecuteAt = next
		}
	}
	return nil
}

func ts2Time(timestamp uint64) types.Time {
	duration := time.Duration(math.Pow10(9-int(types.DefaultFsp))) * time.Nanosecond
	t := model.TSConvert2Time(timestamp)
	t.Truncate(duration)
	return types.NewTime(types.FromGoTime(t), mysql.TypeDatetime, types.DefaultFsp)
}
