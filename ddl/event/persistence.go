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

package event

import (
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/types"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/model"
	model2 "github.com/pingcap/tidb/ddl/event/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

const (
	insertEventTablePrefix = `INSERT IGNORE INTO mysql.async_event VALUES `

	// EVENT_ID             |    bigint(20)
	// EVENT_NAME           |    varchar(64)
	// EVENT_SCHEMA_ID      |    bigint(20)
	// EVENT_SCHEMA_NAME    |    varchar(64)

	// DEFINER              |    varchar(288)
	// SQL_MODE             |    varchar(8192)
	// TIME_ZONE            |    varchar(64)
	// EVENT_BODY           |    varchar(3)
	// EVENT_TYPE           |    varchar(9)
	// EVENT_DEFINITION     |    longtext

	// EXECUTE_AT           |    datetime
	// STARTS               |    datetime
	// ENDS                 |    datetime
	// INTERVAL_VALUE       |    varchar(256)
	// INTERVAL_UNIT        |    bigint(20)

	// STATUS               |    enum('ENABLED','DISABLED','SLAVESIDE_DISABLED')
	// PRESERVE             |    boolean
	// ORIGINATOR           |    bigint
	// INSTANCE             |    varchar(64)
	// CHARSET              |    varchar(64)
	// COLLATION_CONNECTION |    varchar(64)
	// COLLATION_DATABASE   |    varchar(64)
	// COMMENT              |    varchar(2048)

	// NEXT_EXECUTE_AT      |    datetime
	// CREATED              |    datetime

	insertEventTableValue = `(%d, "%s", %d, "%s",
		"%s", "%s", "%s", "%s", "%s", "%s",
		"%s", "%s", "%s", "%s", "%d",
		"%s", %t, %d, "%s", "%s", "%s","%s", "%s", "%s", NOW())`

	insertEventTableSQL = insertEventTablePrefix + insertEventTableValue

	selectEventTableByIDSQL = `SELECT * FROM mysql.async_event where event_id = %d and event_schema_id = %d`

	selectEventTableByNameSQL = `SELECT * FROM mysql.async_event where event_name = "%s" and event_schema_name = "%s"`

	selectEventTableFetchExecutableEvent = `SELECT * FROM mysql.async_event where status="ENABLED" and (instance = "" or instance = "%s") and NEXT_EXECUTE_AT <= NOW() limit 1 for update`

	selectEventTableFetchNextScheduledEvent = `SELECT * FROM mysql.async_event where status="ENABLED" and (instance = "" or instance = "%s") order by NEXT_EXECUTE_AT limit 1`

	deleteEventTableByIDSQL = `DELETE * FROM mysql.async_event where event_id = %d and event_schema_id = %d`

	updateEventTableByIDSQL = `UPDATE mysql.async_event set STATUS = "%s", NEXT_EXECUTE_AT = "%s" where event_id = %d and event_schema_id = %d`
)

// FetchNextScheduledEvent fetch the next event to be scheduled
func FetchNextScheduledEvent(sctx sessionctx.Context, uuid string) (types.Time, error) {
	sql := fmt.Sprintf(selectEventTableFetchNextScheduledEvent, uuid)
	res, err := getEventInfos(sctx, sql)
	if err != nil {
		return types.ZeroTime, errors.Trace(err)
	}
	if len(res) == 0 {
		// no triggered event waited to run.
		return types.ZeroTime, nil
	}
	return res[0].NextExecuteAt, nil
}

// Claim is used to claim a triggered event int the system table concurrently.
func Claim(sctx sessionctx.Context, uuid string) (string, error) {
	logutil.BgLogger().Info("[event] start claim event")
	// Begin.
	_, err := sctx.(sqlexec.SQLExecutor).ExecuteInternal(context.TODO(), "begin")
	if err != nil {
		return "", errors.Trace(err)
	}
	// Select for update.
	sql := fmt.Sprintf(selectEventTableFetchExecutableEvent, uuid)
	res, err := getEventInfos(sctx, sql)
	if err != nil {
		return "", errors.Trace(err)
	}
	if len(res) == 0 {
		return "", nil
	}

	// Update.
	targetEvent := res[0]
	err = Update(targetEvent, sctx)
	if err != nil {
		return "", errors.Trace(err)
	}
	// Commit.
	_, _, err = sctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL("commit")
	if err != nil {
		logutil.BgLogger().Info("[event] claim event commit fail.")
		return "", errors.Trace(err)
	}
	return targetEvent.Statement, nil
}

// Delete delete a eventInfo in physical system table.
func Delete(e *model2.EventInfo, sctx sessionctx.Context) error {
	sql := fmt.Sprintf(deleteEventTableByIDSQL, e.EventID, e.EventSchemaID)

	logutil.BgLogger().Info("[event] delete from event table", zap.Int64("eventID", e.EventID), zap.Int64("event schema ID", e.EventSchemaID))
	_, err := sctx.(sqlexec.SQLExecutor).ExecuteInternal(context.TODO(), sql)
	return errors.Trace(err)
}

// Update Event
func Update(e *model2.EventInfo, sctx sessionctx.Context) error {
	// compute the next execution time.
	err := e.ComputeNextExecuteUTCTime(sctx)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(updateEventTableByIDSQL, e.Enable.String(), e.NextExecuteAt.String(), e.EventID, e.EventSchemaID)
	logutil.BgLogger().Info("[event] update event table", zap.Int64("eventID", e.EventID), zap.Int64("event schema ID", e.EventSchemaID))
	_, err = sctx.(sqlexec.SQLExecutor).ExecuteInternal(context.TODO(), sql)
	return errors.Trace(err)
}

// Insert store a eventInfo into physical system table --- event.
func Insert(e *model2.EventInfo, sctx sessionctx.Context) error {
	// compute the next execution time.
	err := e.ComputeNextExecuteUTCTime(sctx)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(insertEventTableSQL, e.EventID, e.EventName.O, e.EventSchemaID, e.EventSchemaName.O,
		e.Definer.String(), e.SQLMode.String(), e.TimeZone, e.BodyType, e.EventType, e.Statement,
		e.ExecuteAt.String(), e.Starts.String(), e.Ends.String(), e.IntervalValue, e.IntervalUnit,
		e.Enable.String(), e.Preserve, e.Originator, e.Instance, e.Charset, e.CollationConnection, e.CollationDatabase, e.Comment, e.NextExecuteAt.String())

	logutil.BgLogger().Info("[event] insert into event table", zap.Int64("eventID", e.EventID), zap.Int64("event schema ID", e.EventSchemaID))
	_, err = sctx.(sqlexec.SQLExecutor).ExecuteInternal(context.TODO(), sql)
	return errors.Trace(err)
}

// CheckExist checks if exists.
func CheckExist(sctx sessionctx.Context, ident ast.Ident) (*model2.EventInfo, error) {
	return GetFromName(sctx, ident.Name.L, ident.Schema.L)
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
	if len(res) != 1 {
		return nil, nil
	}
	return res[0], nil
}

// ScanEventInfo fetch all valid *eventInfos limited by sql.
func ScanEventInfo(sctx sessionctx.Context, sql string) ([]*model2.EventInfo, error) {
	return getEventInfos(sctx, sql)
}

func getEventInfos(sctx sessionctx.Context, sql string) ([]*model2.EventInfo, error) {
	res, err := sctx.(sqlexec.SQLExecutor).ExecuteInternal(context.TODO(), sql)
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
	var err error
	e.SQLMode, err = mysql.GetSQLMode(r.GetString(5))
	if err != nil {
		logutil.BgLogger().Info("[event] event SQL mode was invalid, restoring default SQL mode")
		if e.SQLMode, err = mysql.GetSQLMode(mysql.DefaultSQLMode); err != nil {
			logutil.BgLogger().Fatal("[event] failed to set SQL mode")
		}
	}
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
	e.CollationConnection = r.GetString(20)
	e.CollationDatabase = r.GetString(21)
	e.Comment = r.GetString(22)

	e.NextExecuteAt = r.GetTime(23)
	return e
}
