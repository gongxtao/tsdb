package chunkenc

import (
	"github.com/gongxtao/tsdb/chunkenc/encode"
	"encoding/binary"
	"fmt"
)

type IntegerChunk struct {
	b	*encode.BStream
}

func NewIntegerChunk() *IntegerChunk {
	i := &IntegerChunk{ b: &encode.BStream{ Stream: make([]byte, 3, 128), Count: 0} }

	// use high bit
	i.b.Bytes()[0] = byte(EncInt64 << 4)

	return i
}

func (c *IntegerChunk) Bytes() []byte { return c.b.Bytes()}
func (c *IntegerChunk) Encoding() Encoding { return EncInt64 }
func (c *IntegerChunk) Appender() (Appender, error) {
	it := c.iterator()
	for it.Next() { 	}
	if it.Err() != nil {
		return nil, it.Err()
	}

	app := &IntegerAppender{
		b: c.b,
		te: encode.NewTimestampEncoder(c.b, it.tde.Delta(), it.tde.At()),
		last: it.At().(*Int64Value),
	}

	return app, nil
}

func (c *IntegerChunk) iterator() *IntegerIterator {
	// total point
	total := binary.BigEndian.Uint16(c.b.Bytes()[1:])
	// really data
	rb := encode.NewBReader(c.b.Bytes()[3:])

	tde := encode.NewTimestampDecoder(rb, total)

	return &IntegerIterator{
		b: rb,
		tde: tde,
		total: total,
		cur: &Int64Value{0, 0},
		read: 0,
		err: nil,
	}
}

func (c *IntegerChunk) Iterator() Iterator { return c.iterator() }
func (c *IntegerChunk) NumSamples() int { return int(binary.BigEndian.Uint16(c.b.Bytes())) }

type IntegerAppender struct{
	b 		*encode.BStream
	te 		*encode.TimestampEncoder

	last 	*Int64Value
}

func (i *IntegerAppender) Append(value Value) error {
	v, ok := value.(*Int64Value)
	if !ok {
		return fmt.Errorf("not support value type, %T", value)
	}

	num := binary.BigEndian.Uint16(i.b.Bytes()[1:])

	// encode timestamp
	i.te.Encode(v.t, num)

	dv := v.v - i.last.v
	enc := encode.ZigZagEncode(dv)
	buf := make([]byte, binary.MaxVarintLen64)
	for _, b := range buf[:binary.PutUvarint(buf, enc)] {
		i.b.WriteByte(b)
	}

	binary.BigEndian.PutUint16(i.b.Bytes()[1:], num + 1)
	i.last = v

	return nil
}

// bstream is really data but len(values)
type IntegerIterator struct {
	b 		*encode.BStream
	tde 	*encode.TimestampDecoder
	total 	uint16
	read 	uint16

	cur 	*Int64Value
	err 	error
}

func (i *IntegerIterator) At() Value {
	return i.cur
}

func (i *IntegerIterator) Err() error {
	return i.err
}

func (i *IntegerIterator) Next() bool {
	if i.read >= i.total || i.err != nil {
		return false
	}

	if ! i.tde.Next() || i.tde.Err() != nil {
		i.err = i.tde.Err()
		return false
	}

	dv, err := binary.ReadUvarint(i.b)
	if err != nil {
		i.err = err
		return false
	}

	i.cur = &Int64Value{t : i.tde.At(), v : i.cur.v + encode.ZigZagDecode(dv)}
	i.read ++
	return true
}

