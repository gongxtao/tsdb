package chunkenc

import (
	"github.com/gongxtao/tsdb/chunkenc/encode"
	"encoding/binary"
	"fmt"
	"github.com/golang/snappy"
)

type StringChunk struct {
	b 	*encode.BStream
}

func NewStringChunk() *StringChunk {
	s := &StringChunk{
		b: encode.NewBWriter(1024, 3),
	}

	// use high bit
	s.b.Bytes()[0] = byte(EncString << 4)

	return s
}

func (c *StringChunk) Bytes() []byte { return c.b.Bytes()}
func (c *StringChunk) Encoding() Encoding { return EncString }
func (c *StringChunk) Appender() (Appender, error) {
	it := c.iterator()
	for it.Next() { 	}
	if it.Err() != nil {
		return nil, it.Err()
	}

	app := &StringAppender{
		b: c.b,
		te: encode.NewTimestampEncoder(c.b, it.tde.Delta(), it.tde.At()),
		last: it.At().(*StringValue),
	}

	return app, nil
}

func (c *StringChunk) iterator() *StringIterator {
	// total point
	total := binary.BigEndian.Uint16(c.b.Bytes()[1:])
	// really data
	rb := encode.NewBReader(c.b.Bytes()[3:])

	tde := encode.NewTimestampDecoder(rb, total)

	return &StringIterator{
		b: rb,
		tde: tde,
		total: total,
		cur: &StringValue{0, ""},
		read: 0,
		err: nil,
	}
}

func (c *StringChunk) Iterator() Iterator { return c.iterator() }
func (c *StringChunk) NumSamples() int { return int(binary.BigEndian.Uint16(c.b.Bytes())) }

type StringAppender struct {
	b 		*encode.BStream

	te 		*encode.TimestampEncoder

	last 	*StringValue
}

func (a *StringAppender) Append(value Value) error {
	v, ok := value.(*StringValue)
	if !ok {
		return fmt.Errorf("not support value type, %T", value)
	}

	num := binary.BigEndian.Uint16(a.b.Bytes()[1:])
	a.te.Encode(v.t, num)

	bs := snappy.Encode(nil, []byte(v.v))
	// compressed length,data
	buf := make([]byte, binary.MaxVarintLen64)
	for _, b := range buf[:binary.PutUvarint(buf, uint64(len(bs)))] {
		a.b.WriteByte(b)
	}
	for _, b := range bs {
		a.b.WriteByte(b)
	}

	a.last = v
	binary.BigEndian.PutUint16(a.b.Bytes()[1:], num + 1)
	return nil
}

// b is pos for start from data
type StringIterator struct {
	b 		*encode.BStream
	tde 	*encode.TimestampDecoder
	total 	uint16
	read 	uint16

	cur 	*StringValue
	err 	error
}

func (i *StringIterator) At() Value {
	return i.cur
}

func (i *StringIterator) Err() error {
	return i.err
}

func (i *StringIterator) Next() bool {
	if i.read >= i.total || i.Err() != nil {
		return false
	}

	// read time
	if ! i.tde.Next() || i.tde.Err() != nil {
		i.err = i.tde.Err()
		return false
	}

	// compressed length
	length, err := binary.ReadUvarint(i.b)
	if err != nil {
		i.err = err
		return false
	}
	// read compressed data
	buf := make([]byte, length)
	x := uint64(0)
	for ; x < length; x ++{
		b, err := i.b.ReadByte()
		if err != nil {
			i.err = err
			return false
		}

		buf[x] = b
	}

	// decode compressed data
	v, err := snappy.Decode(nil, buf[:x])
	if err != nil {
		i.err = fmt.Errorf("snappy failed decode, %v", err)
		return false
	}

	i.read ++
	i.cur = &StringValue{t : i.tde.At(), v: string(v)}

	return i.err == nil
}

