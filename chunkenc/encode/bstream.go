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

// The code in this file was largely written by Damian Gryski as part of
// https://github.com/dgryski/go-tsz and published under the license below.
// It received minor modifications to suit Prometheus's needs.

// Copyright (c) 2015,2016 Damian Gryski <damian@gryski.com>
// All rights reserved.

// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:

// * Redistributions of source code must retain the above copyright notice,
// this list of conditions and the following disclaimer.
//
// * Redistributions in binary form must reproduce the above copyright notice,
// this list of conditions and the following disclaimer in the documentation
// and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
// CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
// OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package encode

import "io"

// bStream is a Stream of bits.
type BStream struct {
	Stream []byte // the data Stream
	Count  uint8  // how many bits are valid in current byte
}

func NewBReader(b []byte) *BStream {
	return &BStream{Stream: b, Count: 8}
}

func NewBWriter(cap int, len int) *BStream {
	return &BStream{Stream: make([]byte, len, cap), Count: 0}
}

func (b *BStream) Clone() *BStream {
	d := make([]byte, len(b.Stream))
	copy(d, b.Stream)
	return &BStream{Stream: d, Count: b.Count}
}

func (b *BStream) Bytes() []byte {
	return b.Stream
}

type bit bool

const (
	Zero bit = false
	One  bit = true
)

func (b *BStream) WriteBit(bit bit) {
	if b.Count == 0 {
		b.Stream = append(b.Stream, 0)
		b.Count = 8
	}

	i := len(b.Stream) - 1

	if bit {
		b.Stream[i] |= 1 << (b.Count - 1)
	}

	b.Count--
}

func (b *BStream) WriteByte(byt byte) {
	if b.Count == 0 {
		b.Stream = append(b.Stream, 0)
		b.Count = 8
	}

	i := len(b.Stream) - 1

	// fill up b.b with b.Count bits from byt
	b.Stream[i] |= byt >> (8 - b.Count)

	b.Stream = append(b.Stream, 0)
	i++
	b.Stream[i] = byt << b.Count
}

func (b *BStream) WriteBits(u uint64, nbits int) {
	u <<= (64 - uint(nbits))
	for nbits >= 8 {
		byt := byte(u >> 56)
		b.WriteByte(byt)
		u <<= 8
		nbits -= 8
	}

	for nbits > 0 {
		b.WriteBit((u >> 63) == 1)
		u <<= 1
		nbits--
	}
}

func (b *BStream) ReadBit() (bit, error) {
	if len(b.Stream) == 0 {
		return false, io.EOF
	}

	if b.Count == 0 {
		b.Stream = b.Stream[1:]

		if len(b.Stream) == 0 {
			return false, io.EOF
		}
		b.Count = 8
	}

	d := (b.Stream[0] << (8 - b.Count)) & 0x80
	b.Count--
	return d != 0, nil
}

func (b *BStream) ReadByte() (byte, error) {
	return b.readByte()
}

func (b *BStream) readByte() (byte, error) {
	if len(b.Stream) == 0 {
		return 0, io.EOF
	}

	if b.Count == 0 {
		b.Stream = b.Stream[1:]

		if len(b.Stream) == 0 {
			return 0, io.EOF
		}
		return b.Stream[0], nil
	}

	if b.Count == 8 {
		b.Count = 0
		return b.Stream[0], nil
	}

	byt := b.Stream[0] << (8 - b.Count)
	b.Stream = b.Stream[1:]

	if len(b.Stream) == 0 {
		return 0, io.EOF
	}

	// We just advanced the Stream and can assume the shift to be 0.
	byt |= b.Stream[0] >> b.Count

	return byt, nil
}

func (b *BStream) ReadBits(nbits int) (uint64, error) {
	var u uint64

	for nbits >= 8 {
		byt, err := b.readByte()
		if err != nil {
			return 0, err
		}

		u = (u << 8) | uint64(byt)
		nbits -= 8
	}

	if nbits == 0 {
		return u, nil
	}

	if nbits > int(b.Count) {
		u = (u << uint(b.Count)) | uint64((b.Stream[0]<<(8-b.Count))>>(8-b.Count))
		nbits -= int(b.Count)
		b.Stream = b.Stream[1:]

		if len(b.Stream) == 0 {
			return 0, io.EOF
		}
		b.Count = 8
	}

	u = (u << uint(nbits)) | uint64((b.Stream[0]<<(8-b.Count))>>(8-uint(nbits)))
	b.Count -= uint8(nbits)
	return u, nil
}
