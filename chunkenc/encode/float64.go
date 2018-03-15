package encode

import (
	"math"
	"math/bits"
)

type Float64Encoder struct {
	b	*BStream

	v      float64

	leading  uint8
	trailing uint8

	num 	uint16
}

func NewFloat64Encoder(sz int) *Float64Encoder {
	return &Float64Encoder{
		b: NewBWriter(sz),
		num: 0,
		leading: 0,
		trailing: 0,
	}
}

func (e *Float64Encoder) Append(v float64) {
	if e.num == 0 {
		e.b.WriteBits(math.Float64bits(v), 64)
	} else {
		e.writeVDelta(v)
	}

	e.num ++
	e.v = v
}

func (e *Float64Encoder) Bytes() ([]byte, error) {
	return e.b.Bytes(), nil
}

func (e *Float64Encoder) Reset() {
	e.b = NewBWriter(0)
	e.v = 0
	e.leading = 0
	e.trailing = 0
	e.num = 0
}

func (e *Float64Encoder) writeVDelta(v float64) {
	vDelta := math.Float64bits(v) ^ math.Float64bits(e.v)

	if vDelta == 0 {
		e.b.WriteBit(Zero)
		return
	}
	e.b.WriteBit(One)

	leading := uint8(bits.LeadingZeros64(vDelta))
	trailing := uint8(bits.TrailingZeros64(vDelta))

	// Clamp number of leading zeros to avoid overflow when encoding.
	if leading >= 32 {
		leading = 31
	}

	if e.leading != 0xff && leading >= e.leading && trailing >= e.trailing {
		e.b.WriteBit(Zero)
		e.b.WriteBits(vDelta>>e.trailing, 64-int(e.leading)-int(e.trailing))
	} else {
		e.leading, e.trailing = leading, trailing

		e.b.WriteBit(One)
		e.b.WriteBits(uint64(leading), 5)

		// Note that if leading == trailing == 0, then sigbits == 64.  But that value doesn't actually fit into the 6 bits we have.
		// Luckily, we never need to encode 0 significant bits, since that would put us in the other case (vdelta == 0).
		// So instead we write out a 0 and adjust it back to 64 on unpacking.
		sigbits := 64 - leading - trailing
		e.b.WriteBits(uint64(sigbits), 6)
		e.b.WriteBits(vDelta>>trailing, int(sigbits))
	}
}

type Float64Decoder struct {
	b       *BStream
	total 	uint16
	read  	uint16

	val 	float64

	leading  	uint8
	trailing 	uint8

	err    		error
}

func (it *Float64Decoder) At() float64 {
	return it.val
}

func (it *Float64Decoder) Err() error {
	return it.err
}

func (it *Float64Decoder) Next() bool {
	if it.err != nil || it.read == it.total {
		return false
	}

	if it.read == 0 {
		v, err := it.b.ReadBits(64)
		if err != nil {
			it.err = err
			return false
		}
		it.val = math.Float64frombits(v)

		it.read ++
		return true
	}

	return it.readValue()
}

func (it *Float64Decoder) readValue() bool {
	bit, err := it.b.ReadBit()
	if err != nil {
		it.err = err
		return false
	}

	if bit == Zero {
		// it.val = it.val
	} else {
		bit, err := it.b.ReadBit()
		if err != nil {
			it.err = err
			return false
		}
		if bit == Zero {
			// reuse leading/trailing zero bits
			// it.leading, it.trailing = it.leading, it.trailing
		} else {
			bits, err := it.b.ReadBits(5)
			if err != nil {
				it.err = err
				return false
			}
			it.leading = uint8(bits)

			bits, err = it.b.ReadBits(6)
			if err != nil {
				it.err = err
				return false
			}
			mbits := uint8(bits)
			// 0 significant bits here means we overflowed and we actually need 64; see comment in encoder
			if mbits == 0 {
				mbits = 64
			}
			it.trailing = 64 - it.leading - mbits
		}

		mbits := int(64 - it.leading - it.trailing)
		bits, err := it.b.ReadBits(mbits)
		if err != nil {
			it.err = err
			return false
		}
		vbits := math.Float64bits(it.val)
		vbits ^= (bits << it.trailing)
		it.val = math.Float64frombits(vbits)
	}

	it.read++

	return true
}
