package tsm1

// #cgo CFLAGS: -I../../../../machete/include
// #cgo LDFLAGS: -L../../../../lib -lmachete
// #include "Machete_C.h"
// #include <string.h>
import "C"

/*
This code is originally from: https://github.com/dgryski/go-tsz and has been modified to remove
the timestamp compression functionality.

It implements the float compression as presented in: http://www.vldb.org/pvldb/vol8/p1816-teller.pdf.
This implementation uses a sentinel value of NaN which means that float64 NaN cannot be stored using
this version.
*/

import (
	"fmt"
	"unsafe"

	"github.com/influxdata/influxdb/v2/tsdb"
)

// Note: an uncompressed format is not yet implemented.
// floatCompressedGorilla is a compressed format using the gorilla paper encoding
const floatCompressedGorilla = 1
const floatCompressedMachete = 2

// uvnan is the constant returned from math.NaN().
const uvnan = 0x7FF8000000000001

var error_bound float64

// FloatEncoder encodes multiple float64s into a byte slice.
type FloatEncoder struct {
	values []float64
	err    error
	result []uint32
}

// NewFloatEncoder returns a new FloatEncoder.
func NewFloatEncoder() *FloatEncoder {
	s := FloatEncoder{
		values: make([]float64, 0, tsdb.DefaultMaxPointsPerBlock),
		err:    nil,
		result: make([]uint32, tsdb.DefaultMaxPointsPerBlock*2),
	}

	return &s
}

// Reset sets the encoder back to its initial state.
func (s *FloatEncoder) Reset() {
	s.values = s.values[:0]
}

// Bytes returns a copy of the underlying byte buffer used in the encoder.
// func (s *FloatEncoder) Bytes() ([]byte, error) {
// 	var result []byte
// 	header := (*reflect.SliceHeader)(unsafe.Pointer(&result))
// 	header.Len = len(s.result) * 4
// 	header.Cap = cap(s.result) * 4
// 	header.Data = uintptr(unsafe.Pointer(&s.result[0]))
// 	return result, s.err
// }
func (s *FloatEncoder) Bytes() ([]byte, error) {
	if s.err != nil {
		return nil, s.err
	} else {
		result := make([]byte, len(s.result)*4)
		C.memcpy(unsafe.Pointer(&result[0]), unsafe.Pointer(&s.result[0]), C.size_t(len(result)))
		return result, nil
	}
}

// Flush indicates there are no more values to encode.
func (s *FloatEncoder) Flush() {
	din := (*C.double)(unsafe.Pointer(&s.values[0]))
	din_len := C.int32_t(len(s.values))
	encoder := C.MachetePrepare(din, din_len, C.double(error_bound))
	out_len := C.MacheteGetSize(encoder)
	// fmt.Printf("Calling Machete on %v %v -> %v\n", din, din_len, out_len)
	if cap(s.result) < int(out_len)+1 {
		s.result = make([]uint32, int(out_len)+1)
	} else {
		s.result = s.result[:int(out_len)+1]
	}
	s.result[0] = floatCompressedMachete | (uint32(len(s.values)) << 8)
	out_len = C.MacheteEncode(encoder, (*C.uint32_t)(&s.result[1]))
	s.result = s.result[:int(out_len)+1]
}

// Write encodes v to the underlying buffer.
func (s *FloatEncoder) Write(v float64) {
	s.values = append(s.values, v)
}

// FloatDecoder decodes a byte slice into multiple float64 values.
type FloatDecoder struct {
	values []float64
	cur    int
	err    error
}

// SetBytes initializes the decoder with b. Must call before calling Next().
func (it *FloatDecoder) SetBytes(b []byte) error {
	if b[0] != floatCompressedMachete {
		it.err = fmt.Errorf("Error in compression type")
		return it.err
	}
	vlen := int(b[1]) | (int(b[2]) << 8) | (int(b[3]) << 16)
	if cap(it.values) < vlen {
		it.values = make([]float64, vlen)
	} else {
		it.values = it.values[:vlen]
	}
	C.MacheteDecode((*C.uint32_t)(unsafe.Pointer(&b[4])), C.uint32_t(len(b)-4),
		(*C.double)(unsafe.Pointer(&it.values[0])), C.uint32_t(vlen))
	it.cur = 0
	return nil
}

// Next returns true if there are remaining values to read.
func (it *FloatDecoder) Next() bool {
	return it.err == nil && it.cur < len(it.values)
}

// Values returns the current float64 value.
func (it *FloatDecoder) Values() float64 {
	v := it.values[it.cur]
	it.cur += 1
	return v
}

// Error returns the current decoding error.
func (it *FloatDecoder) Error() error {
	return it.err
}
