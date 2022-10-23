package tsm1

// #include <stddef.h>
// #include <stdlib.h>
// #include <string.h>
// #include <sys/types.h>
// #include <sys/ipc.h>
// #include <sys/msg.h>
//
// extern int enmqin;
// extern int enmqout;
// extern int demqin;
// extern int demqout;
// extern long comp_id;
// extern long decomp_id;
// struct msg_de {
//         long id;
//         long size;
//	   double error_bound;
//         double data[1022];
// };
// struct msg_en {
//         long id;
//         long size;
//         long length;
//         unsigned char data[8168];
// };
// struct msg_en* compress(void* data, size_t len, double eb);
// struct msg_de* decompress(void* data, size_t size, size_t len);
import "C"

import (
	"errors"
	"unsafe"
)

// FloatArrayEncodeAll encodes src into b, returning b and any error encountered.
// The returned slice may be of a different length and capacity to b.
//
// Currently only the float compression scheme used in Facebook's Gorilla is
// supported, so this method implements a batch oriented version of that.
func FloatArrayEncodeAll(src []float64, b []byte) ([]byte, error) {
	if len(src) <= 20 {
		b = make([]byte, len(src)*8+1)
		b[0] = floatCompressedNone
		C.memcpy(
			unsafe.Pointer(&b[1]),
			unsafe.Pointer(&src[0]),
			C.size_t(len(src)*8))
	} else {
		msg := C.compress(unsafe.Pointer(&src[0]), C.size_t(len(src)), C.double(error_bound))
		defer C.free(unsafe.Pointer(msg))

		b = make([]byte, int(msg.size)+3)
		b[0] = floatCompressedSZ
		b[1] = byte(len(src) & 0xff)
		b[2] = byte(len(src) >> 8)
		C.memcpy(
			unsafe.Pointer(&b[3]),
			unsafe.Pointer(&msg.data[0]),
			C.size_t(msg.size))
	}
	return b, nil
}

func FloatArrayDecodeAll(b []byte, buf []float64) ([]float64, error) {
	if b[0] == floatCompressedNone {
		buf = make([]float64, (len(b)-1)/8)
		C.memcpy(
			unsafe.Pointer(&buf[0]),
			unsafe.Pointer(&b[1]),
			C.size_t(len(b)-1))
	} else if b[0] == floatCompressedSZ {
		blen := uint(b[2])
		blen = (blen << 8) + uint(b[1])
		buf = make([]float64, blen)
		msg := C.decompress(
			(unsafe.Pointer(&b[3])),
			C.size_t(len(b)-3),
			C.size_t(blen))
		defer C.free(unsafe.Pointer(msg))
		C.memcpy(unsafe.Pointer(&buf[0]),
			unsafe.Pointer(&msg.data[0]),
			C.size_t(blen<<3))
	} else {
		err := errors.New("Unknown compression type")
		return nil, err
	}
	return buf, nil
}
