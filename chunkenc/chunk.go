// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chunkenc

import (
	"fmt"
	"sync"

	"github.com/pkg/errors"
	"github.com/gongxtao/tsdb/chunkenc/encode"
	"math"
	"time"
)

// Encoding is the identifier for a chunk encoding.
type Encoding uint8

func (e Encoding) String() string {
	switch e {
	case EncNone:
		return "none"
	case EncFloat64:
		return "Float64"
	case EncInt64:
		return "Int64"
	}
	return "<unknown>"
}

// The different available chunk encodings.
const (
	EncNone Encoding = iota
	EncFloat64
	EncInt64
	EncBoolean
	EncString
	EncUint64
)

// Chunk holds a sequence of sample pairs that can be iterated over and appended to.
type Chunk interface {
	Bytes() []byte
	Encoding() Encoding
	Appender() (Appender, error)
	Iterator() Iterator
	NumSamples() int
}

// FromData returns a chunk from a byte slice of chunk data.
func FromData(e Encoding, d []byte) (Chunk, error) {
	switch e {
	case EncFloat64:
		return &XORChunk{b: &encode.BStream{Count: 0, Stream: d}}, nil
	case EncInt64:
		return &IntegerChunk{b: &encode.BStream{Count: 0, Stream: d}}, nil
	case EncString:
		return &StringChunk{b: &encode.BStream{Count: 0, Stream: d}}, nil
	}
	return nil, fmt.Errorf("unknown chunk encoding: %d", e)
}

// Appender adds sample pairs to a chunk.
type Appender interface {
	Append(value Value) error
}

// Iterator is a simple iterator that can only get the next value.
type Iterator interface {
	At() Value
	Err() error
	Next() bool
}

// NewNopIterator returns a new chunk iterator that does not hold any data.
func NewNopIterator() Iterator {
	return nopIterator{}
}

type nopIterator struct{}

func (nopIterator) At() Value { return &EmptyValue{} }
func (nopIterator) Next() bool           { return false }
func (nopIterator) Err() error           { return nil }

type Pool interface {
	Put(Chunk) error
	Get(e Encoding, b []byte) (Chunk, error)
}

// Pool is a memory pool of chunk objects.
type pool struct {
	xor sync.Pool
}

func NewPool() Pool {
	return &pool{
		xor: sync.Pool{
			New: func() interface{} {
				return &XORChunk{b: &encode.BStream{}}
			},
		},
	}
}

func (p *pool) Get(e Encoding, b []byte) (Chunk, error) {
	switch e {
	case EncFloat64:
		c := p.xor.Get().(*XORChunk)
		c.b.Stream = b
		c.b.Count = 0
		return c, nil
	case EncInt64:
		c := p.xor.Get().(*IntegerChunk)
		c.b.Stream = b
		c.b.Count = 0
		return c, nil
	case EncString:
		c := p.xor.Get().(*StringChunk)
		c.b.Stream = b
		c.b.Count = 0
		return c, nil
	}
	return nil, errors.Errorf("invalid encoding %q", e)
}

func (p *pool) Put(c Chunk) error {
	switch c.Encoding() {
	case EncFloat64:
		xc, ok := c.(*XORChunk)
		// This may happen often with wrapped chunks. Nothing we can really do about
		// it but returning an error would cause a lot of allocations again. Thus,
		// we just skip it.
		if !ok {
			return nil
		}
		xc.b.Stream = nil
		xc.b.Count = 0
		p.xor.Put(c)
	case EncInt64:
		xc, ok := c.(*IntegerChunk)
		if !ok {
			return nil
		}
		xc.b.Stream = nil
		xc.b.Count = 0
		p.xor.Put(c)
	case EncString:
		xc, ok := c.(*StringChunk)
		if !ok {
			return nil
		}
		xc.b.Stream = nil
		xc.b.Count = 0
		p.xor.Put(c)
	default:
		return errors.Errorf("invalid encoding %q", c.Encoding())
	}
	return nil
}

type Value interface {
	UnixNano() int64

	Value() interface{}

	Size() int

	String() string
}

func NewValue(t int64, value interface{}) Value {
	switch v := value.(type) {
	case float64:
		return &Float64Value{ t: t, v: v }
	case int64:
		return &Int64Value{ t: t, v: v }
	}

	return &EmptyValue{	}
}

type Int64Value struct {
	t 	int64
	v 	int64
}

func (i *Int64Value) UnixNano() int64 { return i.t }
func (i *Int64Value) Value() interface{} { return i.v }
func (i *Int64Value) Size() int { return 16 }
func (i *Int64Value) String() string { return fmt.Sprintf("%v %v", time.Unix(0, i.t), i.Value())}


type Float64Value struct {
	t 	int64
	v 	float64
}

func (i *Float64Value) UnixNano() int64 { return i.t }
func (i *Float64Value) Value() interface{} { return i.v }
func (i *Float64Value) Size() int { return 16 }
func (i *Float64Value) String() string { return fmt.Sprintf("%v %v", time.Unix(0, i.t), i.Value())}

type EmptyValue struct{}

func (e EmptyValue) UnixNano() int64 { return math.MinInt64 }
func (e EmptyValue) Value() interface{} { return nil }
func (e EmptyValue) Size() int { return 0 }
func (e EmptyValue) String() string { return "" }

type StringValue struct {
	t	int64
	v 	string
}

func (i *StringValue) UnixNano() int64 { return i.t }
func (i *StringValue) Value() interface{} { return i.v }
func (i *StringValue) Size() int { return 8 + len(i.v) }
func (i *StringValue) String() string { return fmt.Sprintf("%v %v", time.Unix(0, i.t), i.Value()) }
