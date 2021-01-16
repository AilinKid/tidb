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
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/ddl/event"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
)

// Scheduler is the event scheduler
type Scheduler struct {
	ctx                    context.Context
	uuid                   string
	sessPool               *sessionPool
	eventDDLChangedChannel <-chan struct{}
}

func newScheduler(ctx context.Context, uuid string, pool *sessionPool, eventDDLChangedChannel <-chan struct{}) *Scheduler {
	return &Scheduler{
		ctx:                    ctx,
		uuid:                   uuid,
		sessPool:               pool,
		eventDDLChangedChannel: eventDDLChangedChannel,
	}
}

func (d *ddl) runEventScheduler(eventDDLChangedChannel <-chan struct{}) {
	defer d.wg.Done()
	eventScheduler := newScheduler(d.ctx, d.uuid, d.sessPool, eventDDLChangedChannel)
	eventScheduler.Run()
}

// Run starts the event scheduler.
func (s *Scheduler) Run() {
	// Set the event scheduler running status, avoid blocking the eventDDLChangedChannel write channel.
	EventSchedulerRunning.Store(true)
	defer EventSchedulerRunning.Store(false)

	// Set the max triggering time, at least triggering once every day.
	nextTriggerTime := time.Now().Add(24 * time.Hour)
	// Try to claim events to its best.
	sctx, err := s.sessPool.get()
	if err != nil {
		logutil.BgLogger().Info("[event] scheduler get session from pool fail")
		return
	}
	defer s.sessPool.put(sctx)
	for {
		select {
		// Related ddl events triggered.
		case <-s.eventDDLChangedChannel:
		// Next triggering time triggered.
		case <-time.After(nextTriggerTime.Sub(time.Now())): // TODO overflow check
		// scheduler is dead.
		case <-s.ctx.Done():
			return
		}

		nextTriggerTime = time.Now().Add(24 * time.Hour)
		err = claimTriggeredEvents(sctx, s.uuid)
		if err != nil {
			logutil.BgLogger().Info("[event] claim event from system table fail")
			return
		}

		// Set the next trigger time.
		nextTime, err := findNextTriggerTime(sctx, s.uuid)
		if err != nil {
			logutil.BgLogger().Info("[event] fetch most recent event from system table fail")
			return
		}
		if !nextTime.IsZero() {
			// TODO: timezone?
			nextTriggerTime, err = nextTime.GoTime(time.Local)
			if err != nil {
				logutil.BgLogger().Info("[event] fetch most recent event from system table fail")
				return
			}
		}
	}
}

func claimTriggeredEvents(sctx sessionctx.Context, uuid string) error {
	resultChan := make(chan error, 1)
	for {
		ev, err := event.Claim(sctx, uuid)
		if err != nil {
			if kv.ErrTxnRetryable.Equal(err) || kv.ErrWriteConflict.Equal(err) || kv.ErrWriteConflictInTiDB.Equal(err) {
				// the claimed event is done by other TiDB node, retry to fetch other triggered events.
				continue
			}
			return errors.Trace(err)
		}
		if ev == nil {
			// There is no valid triggered events.
			return nil
		}

		// Execute event action.
		sqlexec.RunSQL(context.TODO(), sctx.(sqlexec.SQLExecutor), ev.Statement, false, resultChan)
		event.UpdateEventResult(ev, sctx, <-resultChan)
	}
}

func findNextTriggerTime(sctx sessionctx.Context, uuid string) (types.Time, error) {
	return event.FetchNextScheduledEvent(sctx, uuid)
}
