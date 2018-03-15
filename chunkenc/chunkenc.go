package chunkenc

import (
	"fmt"
	"time"
	"math"
	"github.com/prometheus/tsdb/chunkenc/encode"
	"encoding/binary"
)

const (
	bStreamSize	= 128
)

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

type ChunkEnc struct {
	values 	[]Value
}

func NewIntegerChunk(sz int) *ChunkEnc {
	return &ChunkEnc{ values: make([]Value, 0)}
}

func (c *ChunkEnc) Bytes() ([]byte, error) {

	switch c.values[0].(type) {
	case *Float64Value:
		return c.encodeFloat()
	case *Int64Value:

	}


	return []byte{}, fmt.Errorf("not support value type, %T", c.values[0])
}

func (c *ChunkEnc) Encoding() Encoding {
	if len(c.values) == 0 {
		return EncNone
	}

	value := c.values[0]
	switch value.(type) {
	case *Int64Value:
		return EncInt64
	case *Float64Value:
		return EncFloat64
	}

	return EncNone
}

func (c *ChunkEnc) Appender() (Appender, error) {
	return c, nil
}

func (c *ChunkEnc) Iterator() Iterator {
	return nil
}

func (c *ChunkEnc) NumSamples() int {
	return len(c.values)
}


func (c *ChunkEnc) Append(t int64, v interface{}) {
	value := NewValue(t, v)
	if _, ok := value.(*EmptyValue); ok {
		return
	}


	c.values = append(c.values, value)
}

func (c *ChunkEnc) encodeFloat() ([]byte, error) {
	if len(c.values) == 0 {
		return []byte{}, nil
	}

	fenc := encode.NewFloat64Encoder(bStreamSize)
	tenc := encode.NewTimestampEncoder(bStreamSize)

	b, err := c.encodeFloatUsing(tenc, fenc)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (c *ChunkEnc) encodeFloatUsing(tenc *encode.TimestampEncoder, venc *encode.Float64Encoder) ([]byte, error) {

	for _, value := range c.values {
		v := value.(*Float64Value)
		tenc.Append(v.t)
		venc.Append(v.v)
	}

	tb, err := tenc.Bytes()
	if err != nil {
		return nil, err
	}

	vb, err := venc.Bytes()
	if err != nil {
		return nil, err
	}


	return packBlock(EncFloat64, tb, vb), nil
}

func packBlock(typ Encoding, ts []byte, values []byte) []byte {
	// We encode the length of the timestamp block using a variable byte encoding.
	// This allows small byte slices to take up 1 byte while larger ones use 2 or more.
	sz := 1 + binary.MaxVarintLen64 + len(ts) + len(values)
	buf := make([]byte, sz)

	b := buf[:sz]
	b[0] = byte(typ)
	i := binary.PutUvarint(b[1:1+binary.MaxVarintLen64], uint64(len(ts)))
	i += 1

	// block is <len timestamp bytes>, <ts bytes>, <value bytes>
	copy(b[i:], ts)
	// We don't encode the value length because we know it's the rest of the block after
	// the timestamp block.
	copy(b[i+len(ts):], values)
	return b[:i+len(ts)+len(values)]
}

func unpackBlock(buf []byte) (ts, values []byte, err error) {
	// Unpack the timestamp block length
	tsLen, i := binary.Uvarint(buf)
	if i <= 0 {
		err = fmt.Errorf("unpackBlock: unable to read timestamp block length")
		return
	}

	// Unpack the timestamp bytes
	tsIdx := int(i) + int(tsLen)
	if tsIdx > len(buf) {
		err = fmt.Errorf("unpackBlock: not enough data for timestamp")
		return
	}
	ts = buf[int(i):tsIdx]

	// Unpack the value bytes
	values = buf[tsIdx:]
	return
}

