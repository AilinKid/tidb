// Copyright 2020 PingCAP, Inc.
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
	"fmt"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
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
	fmt.Println(s == "SLAVESIDE_DISABLED")
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

// EventBodyType describes what body type of a event it is.
type EventBodyType uint8

const (
	// BodyTypeNone is unknown type.
	BodyTypeNone EventBodyType = iota
	// BodyTypeSQL means the event body is SQL.
	BodyTypeSQL
	// BodyTypeProcedure means the event body is Procedure.
	BodyTypeProcedure
)

func (t EventBodyType) String() string {
	switch t {
	case BodyTypeSQL:
		return "SQL"
	case BodyTypeProcedure:
		return "PROCEDURE"
	default:
		return "UNKNOWN"
	}
}

// FormEventBodyType form a EventBodyType from a string.
func FormEventBodyType(s string) EventBodyType {
	switch s {
	case "SQL":
		return BodyTypeSQL
	case "PROCEDURE":
		return BodyTypeProcedure
	default:
		return BodyTypeNone

	}
}

// TODO: move this part to parser.
// EventInfo describes what event is like.
type EventInfo struct {
	EventID         int64
	EventName       model.CIStr
	EventSchemaID   int64
	EventSchemaName model.CIStr

	Definer *auth.UserIdentity
	// @@sql_mode when the event was created
	SQLMode mysql.SQLMode
	// @@time_zone when the event was created
	TimeZone string
	Type     EventBodyType
	// the statement to execute
	Statement string
	// the statement to display in SHOW CREATE EVENT / SHOW EVENTS, with secret stuff wiped out
	SecureStmt string
	Enable     EventEnableType
	// start TS in UTC
	Starts types.Time
	// end TS in UTC
	Ends types.Time
	// repeat interval ("EVERY" clause)
	IntervalValue string
	IntervalUnit  ast.TimeUnitType

	// TODO: where to use the following 3 variables? if necessary, add it in system event table definition.
	Preserve bool
	// server ID
	Originator int64
	// UUID of the instance, empty string = any
	Instance string

	Charset   string
	Collation string
	Comment   string
}
