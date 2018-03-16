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
	"io"
	"math/rand"
	"reflect"
	"testing"

	"github.com/prometheus/tsdb/testutil"
	"encoding/binary"
	"strings"
)

type pair struct {
	t int64
	v float64
}

func TestChunk(t *testing.T) {
	for enc, nc := range map[Encoding]struct{
		v func() Chunk
		h func(c Chunk) error
	} {
		EncFloat64: {
			func() Chunk { return NewXORChunk() },
			testFloat64Chunk,
			},
		EncInt64: {
			func() Chunk { return NewIntegerChunk()	},
			testInt64Chunk,
		},
		EncString: {
			func() Chunk { return NewStringChunk()	},
			testStringChunk,
		},
	} {
		t.Run(fmt.Sprintf("%s", enc), func(t *testing.T) {
			for range make([]struct{}, 1) {
				c := nc.v()
				if err := nc.h(c); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func testStringChunk(c Chunk) error {
	app, err := c.Appender()
	if err != nil {
		return err
	}

	var exp []*StringValue
	var (
		ts = int64(1234123324)
		v = "hello world"
	)

	total := 0
	for i := 0; i < 100; i ++ {
		ts += int64(rand.Intn(100000) + 1)
		tv := ""
		if i%2 == 0 {
			tv = fmt.Sprintf("%s", strings.Repeat(v, rand.Intn(100)))
		} else {
			tv = fmt.Sprintf("%s", strings.Repeat(v, rand.Intn(50)))
		}

		total += len(tv)

		if i % 10 == 0 {
			app, err = c.Appender()
			if err != nil {
				return err
			}
		}

		sv := &StringValue{t: ts, v: tv}
		err := app.Append(sv)
		if err != nil {
			return err
		}

		exp = append(exp, sv)
		fmt.Println("appended", len(c.Bytes()), binary.BigEndian.Uint16(c.Bytes()[1:]), "total", total, tv)
	}

	it := c.Iterator()

	var res []*StringValue
	for it.Next() {
		v := it.At().(*StringValue)
		res = append(res, &StringValue{t: v.UnixNano(), v: v.Value().(string)})

	}
	if it.Err() != nil {
		return it.Err()
	}
	if !reflect.DeepEqual(exp, res) {
		return fmt.Errorf("unexpected result\n\ngot: %v\n\nexp: %v", res, exp)
	}

	return nil
}

func testInt64Chunk(c Chunk) error {
	app, err := c.Appender()
	if err != nil {
		return err
	}

	var exp []Int64Value
	var (
		ts = int64(1234123324)
		v  = int64(1243535)
	)
	for i := 0; i < 3000; i++ {
		ts += int64(rand.Intn(10000) + 1)
		// v = rand.Float64()
		if i%2 == 0 {
			v += int64(rand.Intn(1000000))
		} else {
			v -= int64(rand.Intn(1000000))
		}

		// Start with a new appender every 10th sample. This emulates starting
		// appending to a partially filled chunk.
		if i%10 == 0 {
			app, err = c.Appender()
			if err != nil {
				return err
			}
		}

		v := &Int64Value{t: ts, v: v}
		err := app.Append(v)
		if err != nil {
			return err
		}
		exp = append(exp, Int64Value{t: ts, v: v.v})
		b := c.Bytes()
		fmt.Println("appended", len(b), binary.BigEndian.Uint16(b[:2]))
	}

	it := c.Iterator()

	var res []Int64Value
	for it.Next() {
		v := it.At().(*Int64Value)
		res = append(res, Int64Value{t: v.UnixNano(), v: v.Value().(int64)})

	}
	if it.Err() != nil {
		return it.Err()
	}
	if !reflect.DeepEqual(exp, res) {
		return fmt.Errorf("unexpected result\n\ngot: %v\n\nexp: %v", res, exp)
	}


	return nil
}

func testFloat64Chunk(c Chunk) error {
	app, err := c.Appender()
	if err != nil {
		return err
	}

	var exp []pair
	var (
		ts = int64(1234123324)
		v  = 1243535.123
	)
	for i := 0; i < 300; i++ {
		ts += int64(rand.Intn(10000) + 1)
		// v = rand.Float64()
		if i%2 == 0 {
			v += float64(rand.Intn(1000000))
		} else {
			v -= float64(rand.Intn(1000000))
		}

		// Start with a new appender every 10th sample. This emulates starting
		// appending to a partially filled chunk.
		if i%10 == 0 {
			app, err = c.Appender()
			if err != nil {
				return err
			}
		}

		v := &Float64Value{t: ts, v: v}
		app.Append(v)
		exp = append(exp, pair{t: ts, v: v.v})
		b := c.Bytes()
		fmt.Println("appended", len(b), binary.BigEndian.Uint16(b[:2]))
	}

	it := c.Iterator()

	var res []pair
	for it.Next() {
		v := it.At().(*Float64Value)
		res = append(res, pair{t: v.UnixNano(), v: v.Value().(float64)})

	}
	if it.Err() != nil {
		return it.Err()
	}
	if !reflect.DeepEqual(exp, res) {
		return fmt.Errorf("unexpected result\n\ngot: %v\n\nexp: %v", res, exp)
	}
	return nil
}

func benchmarkIterator(b *testing.B, newChunk func() Chunk) {
	var (
		t = int64(1234123324)
		v = 1243535.123
	)
	var exp []pair
	for i := 0; i < b.N; i++ {
		// t += int64(rand.Intn(10000) + 1)
		t += int64(1000)
		// v = rand.Float64()
		v += float64(100)
		exp = append(exp, pair{t: t, v: v})
	}

	var chunks []Chunk
	for i := 0; i < b.N; {
		c := newChunk()

		a, err := c.Appender()
		if err != nil {
			b.Fatalf("get appender: %s", err)
		}
		j := 0
		for _, p := range exp {
			if j > 250 {
				break
			}
			v := &Float64Value{t: p.t, v: p.v}
			a.Append(v)

			i++
			j++
		}
		chunks = append(chunks, c)
	}

	b.ReportAllocs()
	b.ResetTimer()

	fmt.Println("num", b.N, "created chunks", len(chunks))

	res := make([]float64, 0, 1024)

	for i := 0; i < len(chunks); i++ {
		c := chunks[i]
		it := c.Iterator()

		for it.Next() {
			v := it.At()
			res = append(res, v.Value().(float64))
		}
		if it.Err() != io.EOF {
			testutil.Ok(b, it.Err())
		}
		res = res[:0]
	}
}

func BenchmarkXORIterator(b *testing.B) {
	benchmarkIterator(b, func() Chunk {
		return NewXORChunk()
	})
}

func BenchmarkXORAppender(b *testing.B) {
	benchmarkAppender(b, func() Chunk {
		return NewXORChunk()
	})
}

func benchmarkAppender(b *testing.B, newChunk func() Chunk) {
	var (
		t = int64(1234123324)
		v = 1243535.123
	)
	var exp []pair
	for i := 0; i < b.N; i++ {
		// t += int64(rand.Intn(10000) + 1)
		t += int64(1000)
		// v = rand.Float64()
		v += float64(100)
		exp = append(exp, pair{t: t, v: v})
	}

	b.ReportAllocs()
	b.ResetTimer()

	var chunks []Chunk
	for i := 0; i < b.N; {
		c := newChunk()

		a, err := c.Appender()
		if err != nil {
			b.Fatalf("get appender: %s", err)
		}
		j := 0
		for _, p := range exp {
			if j > 250 {
				break
			}

			v := &Float64Value{t: p.t, v: p.v}
			a.Append(v)
			i++
			j++
		}
		chunks = append(chunks, c)
	}

	fmt.Println("num", b.N, "created chunks", len(chunks))
}
