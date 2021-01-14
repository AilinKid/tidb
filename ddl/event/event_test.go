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
	"os"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	*CustomParallelSuiteFlag = true
	logLevel := os.Getenv("log_level")
	logutil.InitLogger(logutil.NewLogConfig(logLevel, logutil.DefaultLogFormat, "", logutil.EmptyFileLogConfig, false))

	config.UpdateGlobal(func(conf *config.Config) {
		conf.Log.SlowThreshold = 30000 // 30s
		conf.TiKVClient.AsyncCommit.SafeWindow = 0
		conf.TiKVClient.AsyncCommit.AllowedClockDrift = 0
		conf.Experimental.AllowsExpressionIndex = true
	})
	tmpDir := config.GetGlobalConfig().TempStoragePath
	_ = os.RemoveAll(tmpDir) // clean the uncleared temp file during the last run.
	_ = os.MkdirAll(tmpDir, 0755)
	testleak.BeforeTest()
	TestingT(t)
	testleak.AfterTestT(t)()
}

type baseTestSuite struct {
	store  kv.Storage
	domain *domain.Domain
	ctx    *mock.Context
}

func (s *baseTestSuite) SetUpSuite(c *C) {
	var err error
	s.store, err = mockstore.NewMockStore()
	c.Assert(err, IsNil)
	s.domain, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
}

func (s *baseTestSuite) TearDownSuite(c *C) {
	s.domain.Close()
	c.Assert(s.store.Close(), IsNil)
}

var _ = Suite(&persistenceSuite{})

type persistenceSuite struct {
	baseTestSuite
}
