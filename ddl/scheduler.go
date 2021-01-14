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

package ddl

import (
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/ddl/event"
	"github.com/pingcap/tidb/sessionctx"
)

type Scheduler struct {
	uuid                   string
	sessPool               *sessionPool
	eventDDLChangedChannel <-chan struct{}
}

func newScheduler(uuid string, pool *sessionPool, c <-chan struct{}) *Scheduler {
	return &Scheduler{
		uuid:                   uuid,
		sessPool:               pool,
		eventDDLChangedChannel: c,
	}
}

func (s *Scheduler) Run() {
	// Set the max triggering time, at least triggering once every day.
	nextTriggerTime := time.Now().Add(24 * time.Hour)
	for {
		select {
		// Related ddl events triggered.
		case <-s.eventDDLChangedChannel:
		// Next triggering time triggered.
		case <-time.After(nextTriggerTime.Sub(time.Now())): // TODO overflow check
		}

		nextTriggerTime = time.Now().Add(24 * time.Hour)

		// Try to complete events to its best.
		sctx, err := s.sessPool.get()
		if err != nil {
			//log
		}
		err = claimTriggeredEvents(sctx, s.uuid)
		if err != nil {
			// TODO: store the error msg.
		}

		findNextTriggerTime(sctx)

		//for _, event := range listAllEvents() {
		//	if event.ExecuteAt <= now {
		//		event.Trigger()
		//		event.Reschedule() // rescheduling may disable or drop the event
		//	}
		//	nextTriggerTime = min(nextTriggerTime, event.ExecuteAt)
		//}
	}
}

func claimTriggeredEvents(sctx sessionctx.Context, uuid string) error {
	for {
		statement, err := event.Claim(sctx, uuid)
		if err != nil {
			return errors.Trace(err)
		}
		if statement != "" {
			// Execute event action.
		} else {
			// There is no valid triggered events.
			return nil
		}
	}
}

func findNextTriggerTime(sctx sessionctx.Context) {

}
