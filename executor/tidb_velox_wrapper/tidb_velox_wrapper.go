package tidb_velox_wrapper

//#cgo CXXFLAGS: -std=c++17
//#cgo CFLAGS: -I/home/guojiangtao/code/velox
//#cgo LDFLAGS: -L${SRCDIR} -ltidb_velox -lvelox_vector -lstdc++ -lvelox_exception -lglog -lgflags -lfolly -lm -lunwind -ldouble-conversion -lfmt -liberty
//
//#include <velox/connectors/tidb/tidb_velox_wrapper.h>
import "C"

type CGoRowVector C.CGoRowVector

type CGoVeloxDataSource struct {
	tidbDataSource *C.CGoTiDBDataSource
	id             int64
}

func NewCGoVeloxDataSource(id int64) *CGoVeloxDataSource {
	s := C.get_tidb_data_source(C.long(id))
	return &CGoVeloxDataSource{
		tidbDataSource: &s,
		id:             id,
	}
}

func (s *CGoVeloxDataSource) Enqueue(data CGoRowVector) {
	C.enqueue_tidb_data_source(C.long(s.id), C.CGoRowVector(data))
}

func (s *CGoVeloxDataSource) Destroy() {
}
