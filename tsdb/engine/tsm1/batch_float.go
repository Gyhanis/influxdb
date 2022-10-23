package tsm1

// #include "Machete_C.h"
// #include <stdlib.h>
// #include <string.h>
import "C"

import (
	"fmt"
	"unsafe"
)

// FloatArrayEncodeAll encodes src into b, returning b and any error encountered.
// The returned slice may be of a different length and capacity to b.
//
// Currently only the float compression scheme used in Facebook's Gorilla is
// supported, so this method implements a batch oriented version of that.
func FloatArrayEncodeAll(src []float64, b []byte) ([]byte, error) {
	din := (*C.double)(unsafe.Pointer(&src[0]))
	din_len := C.int32_t(len(src))
	encoder := C.MachetePrepare(din, din_len, C.double(error_bound))
	out_len := C.MacheteGetSize(encoder)
	out_len2 := int(out_len)*4 + 4
	if cap(b) > out_len2 {
		b = b[:out_len2]
	} else {
		b = make([]byte, out_len2)
	}
	b[0] = floatCompressedMachete
	b[1] = byte(len(src))
	b[2] = byte(len(src) >> 8)
	b[3] = byte(len(src) >> 16)
	C.MacheteEncode(encoder, (*C.uint32_t)(unsafe.Pointer(&b[4])))
	return b, nil
}

func FloatArrayDecodeAll(b []byte, buf []float64) ([]float64, error) {
	if b[0] == floatCompressedMachete {
		vlen := int(b[1]) | (int(b[2]) << 8) | (int(b[3]) << 16)
		if cap(buf) >= vlen {
			buf = buf[:vlen]
		} else {
			buf = make([]float64, vlen)
		}

		din := unsafe.Pointer(&b[4])
		din_len := len(b)/4 - 1
		C.MacheteDecode((*C.uint32_t)(din), C.uint32_t(din_len),
			(*C.double)(unsafe.Pointer(&buf[0])), C.uint32_t(vlen))
		return buf, nil
	} else {
		return nil, fmt.Errorf("Error in compression type")
	}
}
