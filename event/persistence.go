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

package event

import (
	"context"
	"fmt"
	"github.com/pingcap/parser/ast"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	model2 "github.com/pingcap/tidb/event/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

const (
	insertEventTablePrefix = `INSERT IGNORE INTO mysql.async_event VALUES `

	// EVENT_ID          |    bigint(20)
	// EVENT_NAME        |    varchar(64)
	// EVENT_SCHEMA_ID   |    bigint(20)
	// EVENT_SCHEMA_NAME |    varchar(64)

	// DEFINER           |    varchar(288)
	// SQL_MODE          |    bigint(20)
	// TIME_ZONE         |    varchar(64)
	// EVENT_BODY        |    varchar(3)
	// EVENT_TYPE        |    varchar(9)
	// EVENT_DEFINITION  |    longtext

	// EXECUTE_AT        |    datetime
	// STARTS            |    datetime
	// ENDS              |    datetime
	// INTERVAL_VALUE    |    varchar(256)
	// INTERVAL_UINT     |    bigint(20)

	// STATUS            |    enum('ENABLED','DISABLED','SLAVESIDE_DISABLED')
	// PRESERVE          |    boolean
	// ORIGINATOR        |    bigint
	// INSTANCE          |    varchar(64)
	// CHARSET           |    varchar(64)
	// COLLATION         |    varchar(64)
	// COMMENT           |    varchar(2048)

	// NEXT_EXECUTE_AT   |    datetime
	insertEventTableValue = `(%d, "%s", %d, "%s",
		"%s", %d, "%s", "%s", "%s", "%s",
		"%s", "%s", "%s", "%s", "%d",
		"%s", %t, %d, "%s", "%s", "%s", "%s", "%s")`

	insertEventTableSQL = insertEventTablePrefix + insertEventTableValue

	selectEventTableByIDSQL   = `SELECT * FROM mysql.async_event where event_id = %d and event_schema_id = %d`
	selectEventTableByNameSQL = `SELECT * FROM mysql.async_event where event_name = "%s" and event_schema_name = "%s"`

	selectEventTableFetchExecutableEvents = `SELECT * FROM mysql.async_event where status="ENABLED" and (instance = "" or instance = self_uuid) and NEXT_EXECUTE_AT < UTC_TIMESTAMP() limit 1 for update`
)

// Insert store a eventInfo into physical system table --- event.
func Insert(e *model2.EventInfo, sctx sessionctx.Context) error {
	// compute the next execution time.
	err := e.ComputeNextExecuteUTCTime(sctx)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(insertEventTableSQL, e.EventID, e.EventName.O, e.EventSchemaID, e.EventSchemaName.O,
		e.Definer.String(), e.SQLMode, e.TimeZone, e.BodyType, e.EventType, e.Statement,
		e.ExecuteAt.String(), e.Starts.String(), e.Ends.String(), e.IntervalValue, e.IntervalUnit,
		e.Enable.String(), e.Preserve, e.Originator, e.Instance, e.Charset, e.Collation, e.Comment, e.NextExecuteAt)

	logutil.BgLogger().Info("[event] insert into event table", zap.Int64("eventID", e.EventID), zap.String("eventName", e.EventSchemaName.L))
	_, err = sctx.(sqlexec.SQLExecutor).Execute(context.Background(), sql)
	return errors.Trace(err)
}

// GetFromID fetch a *eventInfo with eventID and eventSchemaID via index.
func GetFromID(sctx sessionctx.Context, eventID, eventSchemaID int64) (*model2.EventInfo, error) {
	sql := fmt.Sprintf(selectEventTableByIDSQL, eventID, eventSchemaID)
	logutil.BgLogger().Info("[event] select from event table", zap.Int64("event ID", eventID), zap.Int64("event schema ID", eventSchemaID))
	res, err := getEventInfos(sctx, sql)
	if err != nil {
		return nil, err
	}
	return res[0], nil
}

// GetFromName fetch a *eventInfo with eventName and eventSchemaName via index.
func GetFromName(sctx sessionctx.Context, eventName, eventSchemaName string) (*model2.EventInfo, error) {
	sql := fmt.Sprintf(selectEventTableByNameSQL, eventName, eventSchemaName)
	logutil.BgLogger().Info("[event] select from event table", zap.String("event Name", eventName), zap.String("event schema Name", eventSchemaName))
	res, err := getEventInfos(sctx, sql)
	if err != nil {
		return nil, err
	}
	return res[0], nil
}

// ScanEventInfo fetch all valid *eventInfos limited by sql.
func ScanEventInfo(sctx sessionctx.Context, sql string) ([]*model2.EventInfo, error) {
	return getEventInfos(sctx, sql)
}

func getEventInfos(sctx sessionctx.Context, sql string) ([]*model2.EventInfo, error) {
	res, err := sctx.(sqlexec.SQLExecutor).Execute(context.Background(), sql)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// Single query only has single result.
	rs := res[0]
	defer terror.Call(rs.Close)
	req := rs.NewChunk()
	eventInfos := make([]*model2.EventInfo, 0, req.NumRows())
	for {
		err = rs.Next(context.TODO(), req)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if req.NumRows() == 0 {
			return eventInfos, nil
		}
		it := chunk.NewIterator4Chunk(req)
		for row := it.Begin(); row != it.End(); row = it.Next() {
			eventInfos = append(eventInfos, DecodeRowIntoEventInfo(new(model2.EventInfo), row))
		}
		// NOTE: decodeTableRow decodes data from a chunk Row, that is a shallow copy.
		// The result will reference memory in the chunk, so the chunk must not be reused
		// here, otherwise some werid bug will happen!
		req = chunk.Renew(req, sctx.GetSessionVars().MaxChunkSize)
	}
}

// DecodeRowIntoEventInfo decode a *eventInfo from one row.
func DecodeRowIntoEventInfo(e *model2.EventInfo, r chunk.Row) *model2.EventInfo {
	e.EventID = r.GetInt64(0)
	e.EventName = model.NewCIStr(r.GetString(1))
	e.EventSchemaID = r.GetInt64(2)
	e.EventSchemaName = model.NewCIStr(r.GetString(3))

	auths := strings.Split(r.GetString(4), "@")
	e.Definer = &auth.UserIdentity{Username: auths[0], Hostname: auths[1]}
	e.SQLMode = mysql.SQLMode(r.GetInt64(5))
	e.TimeZone = r.GetString(6)
	e.BodyType = r.GetString(7)
	e.EventType = r.GetString(8)
	e.Statement = r.GetString(9)

	e.ExecuteAt = r.GetTime(10)
	e.Starts = r.GetTime(11)
	e.Ends = r.GetTime(12)
	e.IntervalValue = r.GetString(13)
	e.IntervalUnit = ast.TimeUnitType(r.GetInt64(14))

	e.Enable = model2.FormEventEnableType(r.GetEnum(15).String())
	e.Preserve = r.GetInt64(16) == 1
	e.Originator = r.GetInt64(17)
	e.Instance = r.GetString(18)
	e.Charset = r.GetString(19)
	e.Collation = r.GetString(20)
	e.Comment = r.GetString(21)

	e.NextExecuteAt = r.GetTime(22)
	return e
}
