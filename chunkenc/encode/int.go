package encode

import (
	"github.com/jwilder/encoding/simple8b"
	"fmt"
)

const (
	// intUncompressed is an uncompressed format using 8 bytes per point
	intUncompressed = 0
	// intCompressedSimple is a bit-packed format using simple8b encoding
	intCompressedSimple = 1
	// intCompressedRLE is a run-length encoding format
	intCompressedRLE = 2
)

type IntegerEncoder struct {
	prev	int64
	rle    	bool
	values 	[]uint64

	b 		*BStream
}

func NewIntegerEncoder(sz int) *IntegerEncoder {
	return &IntegerEncoder{
		rle: true,
		values: make([]uint64, 0, sz),
		b: NewBWriter(sz),
	}
}

func (e *IntegerEncoder) Append(v int64) {
	// Delta-encode each value as it's written.  This happens before
	// ZigZagEncoding because the deltas could be negative.
	delta := v - e.prev
	e.prev = v
	enc := ZigZagEncode(delta)
	if len(e.values) > 1 {
		e.rle = e.rle && e.values[len(e.values)-1] == enc
	}

	e.values = append(e.values, enc)
}

// Reset sets the encoder back to its initial state.
func (e *IntegerEncoder) Reset() {
	e.prev = 0
	e.rle = true
	e.values = e.values[:0]
}

// Bytes returns a copy of the underlying buffer.
func (e *IntegerEncoder) Bytes() ([]byte, error) {
	// Only run-length encode if it could reduce storage size.
	if e.rle && len(e.values) > 2 {
		return e.encodeRLE()
	}

	for _, v := range e.values {
		// Value is too large to encode using packed format
		if v > simple8b.MaxValue {
			return e.encodeUncompressed()
		}
	}

	return e.encodePacked()
}

func (e *IntegerEncoder) encodeRLE() ([]byte, error) {
	// 4 high bits used for the encoding type
	e.b.WriteByte(byte(intCompressedRLE) << 4)

	// The first value
	e.b.WriteBits(e.values[0], 64)
	// The first delta
	e.b.WriteBits(e.values[1], 64)
	// The number of times the delta is repeated
	e.b.WriteBits(uint64(len(e.values) - 1), 64)

	return e.b.Bytes(), nil
}

func (e *IntegerEncoder) encodePacked() ([]byte, error) {
	if len(e.values) == 0 {
		return nil, nil
	}

	// Encode all but the first value.  Fist value is written unencoded
	// using 8 bytes.
	encoded, err := simple8b.EncodeAll(e.values[1:])
	if err != nil {
		return nil, err
	}

	// 4 high bits of first byte store the encoding type for the block
	e.b.WriteByte(byte(intCompressedSimple) << 4)

	// Write the first value since it's not part of the encoded values
	e.b.WriteBits(e.values[0], 64)

	// Write the encoded values
	for _, v := range encoded {
		e.b.WriteBits(v, 64)
	}
	return e.b.Bytes(), nil
}

func (e *IntegerEncoder) encodeUncompressed() ([]byte, error) {
	if len(e.values) == 0 {
		return nil, nil
	}

	// 4 high bits of first byte store the encoding type for the block
	e.b.WriteByte(byte(intUncompressed) << 4)

	for _, v := range e.values {
		e.b.WriteBits(v, 64)
	}
	return e.b.Bytes(), nil
}

func NewIntegerDecoder(b []byte, total uint16) *IntegerDecoder {
	it := &IntegerDecoder{}

	if total == 0 {
		it.encoding = 0
		it.total = 0
	} else {
		it.encoding = b[0] >> 4
		it.b = NewBReader(b[1:])
	}

	it.read = 0
	it.err = nil
	it.first = true

	return it
}

type IntegerDecoder struct {
	b 		*BStream
	total 	uint16
	read 	uint16

	// 240 is the maximum number of values that can be encoded into a single uint64 using simple8b
	values 	[240]uint64
	i      	int
	n      	int
	prev   	int64
	first  	bool

	// The first value for a run-length encoded byte slice
	rleFirst uint64
	// The delta value for a run-length encoded byte slice
	rleDelta uint64

	encoding byte
	err      error
}

func (it *IntegerDecoder) At() int64 {
	it.read ++

	switch it.encoding {
	case intCompressedRLE:
		return ZigZagDecode(it.rleFirst) + int64(it.i) * ZigZagDecode(it.rleDelta)
	default:
		v := ZigZagDecode(it.values[it.i])
		v = v + it.prev
		it.prev = v

		return v
	}
}

func (it *IntegerDecoder) Err() error {
	return it.err
}

func (it *IntegerDecoder) Next() bool {
	if it.read >= it.total || it.err != nil {
		return false
	}

	// could be cycle read values
	if it.i >= it.n && it.read >= it.total {
		return false
	}

	it.i ++

	if it.i >= it.n {
		switch it.encoding {
		case intCompressedRLE:
		case intCompressedSimple:
		case intUncompressed:
		default:
			it.err = fmt.Errorf("unknown encoding %v", it.encoding)
		}
	}
	
	return it.err == nil
}

// type,delta0,encode deltas[1:]
func (it *IntegerDecoder) decodePacked() {

	v, err := it.b.ReadBits(64)
	if err != nil {
		it.err = err
		return
	}

	if it.first {
		it.first = false

		// the first have not encoded
		it.values[0] = v
		it.n = 1
	} else {
		n, err := simple8b.Decode(&it.values, v)
		if err != nil {
			it.err = fmt.Errorf("failed to decode value, err:%v", err)
		}
		it.n = n
	}

	it.i = 0
}

// type,delta0,delta1,repeated delta(number)
func (it *IntegerDecoder) decodeRle() {

	// read the first value
	first, err := it.b.ReadBits(64)
	if err != nil {
		it.err = err
		return
	}

	// read the delta
	delta, err := it.b.ReadBits(64)
	if err != nil {
		it.err = err
		return
	}

	// read the repeat times for delta
	repeat, err := it.b.ReadBits(64)
	if err != nil {
		it.err = err
		return
	}

	it.rleFirst = first
	it.rleDelta = delta
	it.n = int(repeat) + 1
	it.i = 0
}

// type,delta[0:]
func (it *IntegerDecoder) decodeUncompressed() {
	v, err := it.b.ReadBits(64)
	if err != nil {
		it.err = err
		return
	}

	it.values[0] = v

	it.i = 0
	it.n = 1
}