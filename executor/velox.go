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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"sync"

	"github.com/pingcap/tidb/executor/tidb_velox_wrapper"
	"github.com/pingcap/tidb/util/chunk"
)

var _ Executor = &VeloxExec{}

type VeloxExec struct {
	baseExecutor

	prepared     bool
	tableReaders []*TableReaderExecutor
	workerWg     *sync.WaitGroup

	// Connector to velox, C++ interface.
	// Shared by all veloxWorkers.
	veloxDS *tidb_velox_wrapper.CGoVeloxDataSource
}

func (e *VeloxExec) Open(ctx context.Context) (err error) {
	// TODO: e.id must be unique
	e.veloxDS = tidb_velox_wrapper.NewCGoVeloxDataSource(int64(e.id))
	for _, r := range e.tableReaders {
		if err = r.Open(ctx); err != nil {
			return err
		}
	}
	return err
}

func (e *VeloxExec) Next(ctx context.Context, _ *chunk.Chunk) error {
	if !e.prepared {
		e.startWorkers(ctx)
	}
	return nil
}

func (e *VeloxExec) Close() error {
	var firstErr error
	for _, r := range e.tableReaders {
		if err := r.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	e.workerWg.Wait()
	e.veloxDS.Destroy()
	return firstErr
}

type veloxWorker struct {
	tableReader *TableReaderExecutor

	// Connector to velox, C++ interface.
	// Shared by all veloxWorkers.
	veloxDS *tidb_velox_wrapper.CGoVeloxDataSource

	// ID to distinguish different tableReader.
	sourceID int
	workerWg *sync.WaitGroup
}

// For each tableReader, start a worker to:
// 1. call tableReader.Next() to read chunk
// 2. call ChunkConvertor to convert chunk to Velox::RowVectorPtr
// 3. push Velox::RowVectorPtr to veloxDataSource
func (e *VeloxExec) startWorkers(ctx context.Context) {
	for _, r := range e.tableReaders {
		worker := &veloxWorker{
			tableReader: r,
			veloxDS:     e.veloxDS,
			sourceID:    r.id,
			workerWg:    e.workerWg,
		}
		e.workerWg.Add(1)
		worker.run(ctx)
	}
}

// TODO:
type rowVector struct {
}

func (w *veloxWorker) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			w.workerWg.Done()
		}

		req := newFirstChunk(w.tableReader)
		w.tableReader.Next(ctx, req)

		var arrow tidb_velox_wrapper.CGoRowVector
		// arrow := chunkConvertor.convertToArrow(req)
		// may block. TODO: ctx cancel
		w.veloxDS.Enqueue(arrow)
	}
}
