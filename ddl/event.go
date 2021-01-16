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

package ddl

import (
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/ddl/event"
	model2 "github.com/pingcap/tidb/ddl/event/model"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
)

func (w *worker) onCreateEvent(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	eventInfo := &model2.EventInfo{}
	var execAt, start, end, nextExecAt types.CoreTime
	if err := job.DecodeArgs(eventInfo, &execAt, &start, &end, &nextExecAt); err != nil {
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	eventInfo.ExecuteAt = types.NewTime(execAt, mysql.TypeDatetime, types.DefaultFsp)
	eventInfo.Starts = types.NewTime(start, mysql.TypeDatetime, types.DefaultFsp)
	eventInfo.Ends = types.NewTime(end, mysql.TypeDatetime, types.DefaultFsp)
	eventInfo.NextExecuteAt = types.NewTime(nextExecAt, mysql.TypeDatetime, types.DefaultFsp)
	// Double check schema existence.
	_, err := checkSchemaExistAndCancelNotExistJob(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	// Double check event existence.
	fmt.Println("get1")
	sctx, err := w.sessPool.get()
	if err != nil {
		return ver, errors.Trace(err)
	}
	fmt.Println("get2")
	defer w.sessPool.put(sctx)
	old, err := event.CheckExist(sctx, ast.Ident{Schema: eventInfo.EventSchemaName, Name: eventInfo.EventName})
	if err != nil {
		// Wait for retry.
		return ver, errors.Trace(err)
	}
	fmt.Println("check1")
	if old != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(infoschema.ErrEventExists.GenWithStackByArgs(eventInfo.EventName))
	}

	err = event.Insert(eventInfo, sctx)
	if err != nil {
		return ver, errors.Trace(err)
	}
	fmt.Println("insert1")
	ver, err = updateSchemaVersion(t, job)
	if err != nil {
		// Since t and sctx is in two different txn, so it should clean the inserted event here.
		err1 := event.Delete(eventInfo, sctx)
		if err1 != nil {
			logutil.BgLogger().Warn("[event] clean inserted event from event table failed")
		}
		return ver, errors.Trace(err)
	}
	// Finish this job.
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, nil)
	asyncNotifyEvent(d, &util.Event{Tp: model.ActionCreateEvent})
	return ver, nil
}

func (w *worker) onDropEvent(t *meta.Meta, job *model.Job) (ver int64, _ error) {

	eventInfo := &model2.EventInfo{}
	if err := job.DecodeArgs(eventInfo); err != nil {
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	sctx, err := w.sessPool.get()
	if err != nil {
		return ver, errors.Trace(err)
	}
	defer w.sessPool.put(sctx)
	err = event.Delete(eventInfo, sctx)
	if err != nil {
		return ver, errors.Trace(err)
	}
	ver, err = updateSchemaVersion(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	// Finish this job.
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, nil)
	return ver, nil

}

func (w *worker) onAlterEvent(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	eventInfo := &model2.EventInfo{}
	if err := job.DecodeArgs(eventInfo); err != nil {
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	sctx, err := w.sessPool.get()
	if err != nil {
		return ver, errors.Trace(err)
	}
	defer w.sessPool.put(sctx)

	err = event.UpdateEventComment(eventInfo, sctx)
	if err != nil {
		return ver, errors.Trace(err)
	}
	ver, err = updateSchemaVersion(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	// Finish this job.
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, nil)
	return ver, nil
}
