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

package event_test

import (
	"context"
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/ddl/event"
	model2 "github.com/pingcap/tidb/ddl/event/model"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *persistenceSuite) TestEventSystemTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use mysql")
	res, err := tk.Exec("desc async_event")
	c.Assert(err, IsNil)
	chk := res.NewChunk()
	res.Next(context.TODO(), chk)
	c.Assert(chk.NumRows(), Greater, 0)

	e := &model2.EventInfo{
		EventID:         1,
		EventName:       model.NewCIStr("a"),
		EventSchemaID:   2,
		EventSchemaName: model.NewCIStr("b"),

		Definer:   &auth.UserIdentity{Username: "root", Hostname: "localhost"},
		SQLMode:   mysql.ModeStrictAllTables,
		TimeZone:  "UTC",
		BodyType:  "SQL",
		EventType: "ONE TIME",
		Statement: "select * from t",

		ExecuteAt:     types.CurrentTime(mysql.TypeDatetime),
		Starts:        types.CurrentTime(mysql.TypeDatetime),
		Ends:          types.CurrentTime(mysql.TypeDatetime),
		IntervalValue: "3",
		IntervalUnit:  ast.TimeUnitHour,

		Enable:     model2.TypeEnabled,
		Preserve:   true,
		Originator: 1,
		Instance:   "",
		Charset:    "UTF8",
		Collation:  "UTF8MB4",
		Comment:    "Nothing",
	}
	err = event.Insert(e, tk.Se)
	c.Assert(err, IsNil)

	tk.MustQuery("select count(*) from async_event").Check(testkit.Rows("1"))

	e2, err := event.GetFromID(tk.Se, 1, 2)
	c.Assert(err, IsNil)
	c.Assert(e.EventID, DeepEquals, e2.EventID)
	c.Assert(e.EventName, DeepEquals, e2.EventName)
	c.Assert(e.EventSchemaID, DeepEquals, e2.EventSchemaID)
	c.Assert(e.EventSchemaName, DeepEquals, e2.EventSchemaName)

	// Different instance address, couldn't use deep equal here.
	c.Assert(e.Definer.String(), Equals, e2.Definer.String())
	c.Assert(e.SQLMode, Equals, e2.SQLMode)
	c.Assert(e.TimeZone, Equals, e2.TimeZone)
	c.Assert(e.BodyType, Equals, e2.BodyType)
	c.Assert(e.EventType, Equals, e2.EventType)
	c.Assert(e.Statement, Equals, e2.Statement)
	c.Assert(e.IntervalValue, Equals, e2.IntervalValue)
	c.Assert(e.IntervalUnit, Equals, e2.IntervalUnit)

	// TiDB core time will generated with fsp, datetime(0) type doesn't have fsp, use String() here.
	c.Assert(e.Starts.String(), Equals, e2.Starts.String())
	c.Assert(e.Ends.String(), Equals, e2.Ends.String())
	c.Assert(e.Enable, Equals, e2.Enable)
	c.Assert(e.Preserve, Equals, e2.Preserve)
	c.Assert(e.Originator, Equals, e2.Originator)
	c.Assert(e.Instance, Equals, e2.Instance)
	c.Assert(e.Charset, Equals, e2.Charset)
	c.Assert(e.Collation, Equals, e2.Collation)
	c.Assert(e.Comment, Equals, e2.Comment)

	e3, err := event.GetFromName(tk.Se, "a", "b")
	c.Assert(err, IsNil)
	c.Assert(e.EventID, DeepEquals, e3.EventID)
	c.Assert(e.EventName, DeepEquals, e3.EventName)
	c.Assert(e.EventSchemaID, DeepEquals, e3.EventSchemaID)
	c.Assert(e.EventSchemaName, DeepEquals, e3.EventSchemaName)

	// Different instance address, couldn't use deep equal here.
	c.Assert(e.Definer.String(), Equals, e3.Definer.String())
	c.Assert(e.SQLMode, Equals, e3.SQLMode)
	c.Assert(e.TimeZone, Equals, e3.TimeZone)
	c.Assert(e.BodyType, Equals, e3.BodyType)
	c.Assert(e.EventType, Equals, e3.EventType)
	c.Assert(e.Statement, Equals, e3.Statement)
	c.Assert(e.IntervalValue, Equals, e3.IntervalValue)
	c.Assert(e.IntervalUnit, Equals, e3.IntervalUnit)

	// TiDB core time will generated with fsp, datetime(0) type doesn't have fsp, use String() here.
	c.Assert(e.Starts.String(), Equals, e3.Starts.String())
	c.Assert(e.Ends.String(), Equals, e3.Ends.String())
	c.Assert(e.Enable, Equals, e3.Enable)
	c.Assert(e.Preserve, Equals, e2.Preserve)
	c.Assert(e.Originator, Equals, e2.Originator)
	c.Assert(e.Instance, Equals, e2.Instance)
	c.Assert(e.Charset, Equals, e3.Charset)
	c.Assert(e.Collation, Equals, e3.Collation)
	c.Assert(e.Comment, Equals, e3.Comment)

	statement, err := event.Claim(tk.Se, "")
	c.Assert(err, IsNil)
	c.Assert(statement, Equals, "select * from t")
}
