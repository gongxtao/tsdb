package encode

import (
	"encoding/binary"
)

type TimestampEncoder struct {
	b 		*BStream

	tDelta 	uint64
	t 		int64
}

func NewTimestampEncoder(b *BStream, delta uint64, t int64) *TimestampEncoder {
	return &TimestampEncoder{
		b: b,
		tDelta: delta,
		t: t,
	}
}

func (e *TimestampEncoder) Encode(t int64, encounter uint16) {
	var tDelta uint64

<<<<<<< HEAD
	if encounter == 0 {
=======
	if e.num == 0 {
>>>>>>> 1c613d0e7a9894f83c870fbe58e1bc4250710759
		buf := make([]byte, binary.MaxVarintLen64)
		for _, b := range buf[:binary.PutVarint(buf, t)] {
			e.b.WriteByte(b)
		}

	} else if encounter == 1 {
		tDelta = uint64(t - e.t)

		buf := make([]byte, binary.MaxVarintLen64)
		for _, b := range buf[:binary.PutUvarint(buf, tDelta)] {
			e.b.WriteByte(b)
		}
	} else {

		tDelta = uint64(t - e.t)
		dod := int64(tDelta - e.tDelta)

		// Gorilla has a max resolution of seconds, Prometheus milliseconds.
		// Thus we use higher value range steps with larger bit size.
		switch {
		case dod == 0:
			e.b.WriteBit(Zero)
		case bitRange(dod, 14):
			e.b.WriteBits(0x02, 2) // '10'
			e.b.WriteBits(uint64(dod), 14)
		case bitRange(dod, 17):
			e.b.WriteBits(0x06, 3) // '110'
			e.b.WriteBits(uint64(dod), 17)
		case bitRange(dod, 20):
			e.b.WriteBits(0x0e, 4) // '1110'
			e.b.WriteBits(uint64(dod), 20)
		default:
			e.b.WriteBits(0x0f, 4) // '1111'
			e.b.WriteBits(uint64(dod), 64)
		}
	}

	e.t = t
	e.tDelta = tDelta
}

type TimestampDecoder struct {
	b 		*BStream
	read 	uint16
	total 	uint16

	t 		int64

	tDelta 	uint64
	err 	error
}

func NewTimestampDecoder(b *BStream, total uint16) *TimestampDecoder {
	it := &TimestampDecoder{ b: b }

	if total == 0 {
		it.b = b
		it.total = 0
	} else {
		it.total = total
		it.b = b
	}

	it.read = 0
	return it
}

func (it *TimestampDecoder) Delta() uint64 {
	return it.tDelta
}

func (it *TimestampDecoder) At() int64 {
	return it.t
}

func (it *TimestampDecoder) Err() error {
	return it.err
}

func (it *TimestampDecoder) Next() bool {
	if it.err != nil || it.read == it.total {
		return false
	}

	if it.read == 0 {
		t, err := binary.ReadVarint(it.b)
		if err != nil {
			it.err = err
			return false
		}
		it.t = int64(t)

		it.read++
		return true
	}
	if it.read == 1 {
		tDelta, err := binary.ReadUvarint(it.b)
		if err != nil {
			it.err = err
			return false
		}

		it.tDelta = tDelta
		it.t = it.t + int64(it.tDelta)
		it.read ++

		return true
	}

	var d byte
	// read delta-of-delta
	for i := 0; i < 4; i++ {
		d <<= 1
		bit, err := it.b.ReadBit()
		if err != nil {
			it.err = err
			return false
		}
		if bit == Zero {
			break
		}
		d |= 1
	}
	var sz uint8
	var dod int64
	switch d {
	case 0x00:
		// dod == 0
	case 0x02:
		sz = 14
	case 0x06:
		sz = 17
	case 0x0e:
		sz = 20
	case 0x0f:
		bits, err := it.b.ReadBits(64)
		if err != nil {
			it.err = err
			return false
		}

		dod = int64(bits)
	}

	if sz != 0 {
		bits, err := it.b.ReadBits(int(sz))
		if err != nil {
			it.err = err
			return false
		}
		if bits > (1 << (sz - 1)) {
			// or something
			bits = bits - (1 << sz)
		}
		dod = int64(bits)
	}

	it.tDelta = uint64(int64(it.tDelta) + dod)
	it.t = it.t + int64(it.tDelta)
	return it.err == nil
}

func bitRange(x int64, nbits uint8) bool {
	return -((1<<(nbits-1))-1) <= x && x <= 1<<(nbits-1)
}
