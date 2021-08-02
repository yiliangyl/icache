package icache

import (
	"errors"
	"github.com/golang/protobuf/proto"
)

// Sink receives data from a Get call.
//
// Implementation of Getter must call exactly one of the Set methods
// on success.
type Sink interface {
	// SetString sets the value to s.
	SetString(s string) error

	// SetBytes sets the value to the contents of v.
	// The caller retains ownership of v.
	SetBytes(v []byte) error

	// SetProto sets the value to the encoded version of m.
	// The caller retains ownership of m.
	SetProto(m proto.Message) error

	// view returns a frozen view of the bytes for caching.
	view() (ByteView, error)
}

func cloneBytes(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

// viewSetter is a Sink that can also receive its value from
// a ByteView. This is a fast path to minimize copies when the
// item was already cached locally in memory (where it's
// cached as a ByteView)
type viewSetter interface {
	setView(v ByteView) error
}

func setSinkView(s Sink, v ByteView) error {
	if vs, ok := s.(viewSetter); ok {
		return vs.setView(v)
	}
	if v.b != nil {
		return s.SetBytes(v.b)
	}
	return s.SetString(v.s)
}

// AllocatingByteSliceSink returns a Sink that allocates
// a byte slice to hold the received value and assigns
// it to *dst. The memory is not retained by icache.
func AllocatingByteSliceSink(dst *[]byte) Sink {
	return &allocByteSink{dst: dst}
}

type allocByteSink struct {
	dst *[]byte
	v   ByteView
}

func (s *allocByteSink) SetString(v string) error {
	if s.dst == nil {
		return errors.New("nil AllocatingByteSliceSink *[]byte dst")
	}
	*s.dst = []byte(v)
	s.v.b = nil
	s.v.s = v
	return nil
}

func (s *allocByteSink) SetBytes(b []byte) error {
	return s.setByteOwned(cloneBytes(b))
}

func (s *allocByteSink) SetProto(m proto.Message) error {
	b, err := proto.Marshal(m)
	if err != nil {
		return err
	}
	return s.setByteOwned(b)
}

func (s *allocByteSink) setByteOwned(b []byte) error {
	if s.dst == nil {
		return errors.New("nil AllocatingByteSliceSink *[]byte dst")
	}
	*s.dst = cloneBytes(b)
	s.v.b = b
	s.v.s = ""
	return nil
}

func (s *allocByteSink) view() (ByteView, error) {
	return s.v, nil
}

func (s *allocByteSink) setView(v ByteView) error {
	if v.b != nil {
		*s.dst = cloneBytes(v.b)
	} else {
		*s.dst = []byte(v.s)
	}
	s.v = v
	return nil
}

// StringSink returns a Sink that populates the provided string pointer.
func StringSink(sp *string) Sink {
	return &stringSink{sp: sp}
}

type stringSink struct {
	sp *string
	v  ByteView
}

func (s *stringSink) view() (ByteView, error) {
	return s.v, nil
}

func (s *stringSink) SetString(v string) error {
	s.v.b = nil
	s.v.s = v
	*s.sp = v
	return nil
}

func (s *stringSink) SetBytes(v []byte) error {
	return s.SetString(string(v))
}

func (s *stringSink) SetProto(m proto.Message) error {
	b, err := proto.Marshal(m)
	if err != nil {
		return err
	}
	s.v.b = b
	*s.sp = string(b)
	return nil
}
