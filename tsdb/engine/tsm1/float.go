package tsm1

// #include <stdio.h>
// #include <errno.h>
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
// long comp_id = 1;
// long decomp_id = 1;
// struct msg_de {
//         long id;
//         long size;
//	   double error_bound;
//         double data[1021];
// };
//
// struct msg_en {
//         long id;
//         long size;
//         long length;
//         unsigned char data[8168];
// };
//
// struct msg_en* compress(void* data, size_t len, double eb) {
// 	struct msg_de msg_de;
// 	struct msg_en* msg_en = malloc(sizeof(*msg_en));
//	do {
// 		msg_de.id = __atomic_fetch_add(&comp_id, 1, __ATOMIC_RELAXED);
//		msg_de.id &= 0xfffffffffffffffL;
// 	} while(msg_de.id == 0);
// 	msg_de.size = len;
//	msg_de.error_bound = eb;
// 	memcpy(msg_de.data, data, sizeof(double) * len);
//	long r;
// 	do {
// 		r = msgsnd(enmqin, &msg_de, sizeof(msg_de), 0);
// 	} while (r == -1 && errno == 4);
// 	if (r == -1) printf("Send error: %d\n", errno);
// 	do {
// 		r = msgrcv(enmqout, msg_en, sizeof(*msg_en), msg_de.id, 0);
// 	} while (r == -1 && errno == 4);
// 	if (r == -1) printf("Receive error: %d\n", errno);
// 	return msg_en;
// }
//
// struct msg_de* decompress(void* data, size_t size, size_t len) {
//	int r;
// 	struct msg_en msg_en;
// 	struct msg_de* msg_de = malloc(sizeof(*msg_de));
// 	do {
// 		msg_en.id = __atomic_fetch_add(&decomp_id, 1, __ATOMIC_RELAXED);
// 		msg_en.id &= 0xfffffffffffffffL;
// 	} while(msg_en.id == 0);
// 	msg_en.size = size;
// 	msg_en.length = len;
// 	memcpy(msg_en.data, data, size);
//	do {
//		r = msgsnd(demqin, &msg_en, sizeof(msg_en), 0);
// 	} while (r == -1 && errno == 4);
//	if (r == -1) printf("Msgsnd Error: %d\n", errno);
//	do {
// 		r = msgrcv(demqout, msg_de, sizeof(*msg_de), msg_en.id, 0);
// 	} while (r == -1 && errno == 4);
//	if (r == -1) printf("Msgrcv Error: %d\n", errno);
// 	return msg_de;
// }
import "C"

/*
This code is originally from: https://github.com/dgryski/go-tsz and has been modified to remove
the timestamp compression functionality.

It implements the float compression as presented in: http://www.vldb.org/pvldb/vol8/p1816-teller.pdf.
This implementation uses a sentinel value of NaN which means that float64 NaN cannot be stored using
this version.
*/

import (
	// "fmt"
	// "os"
	// "reflect"
	"errors"
	"unsafe"

	"github.com/influxdata/influxdb/v2/tsdb"
)

// Note: an uncompressed format is not yet implemented.
// floatCompressedGorilla is a compressed format using the gorilla paper encoding
const floatCompressedNone = 0
const floatCompressedGorilla = 1
const floatCompressedSZ = 2

// uvnan is the constant returned from math.NaN().
const uvnan = 0x7FF8000000000001

var error_bound float64

// FloatEncoder encodes multiple float64s into a byte slice.
type FloatEncoder struct {
	buf []float64
}

// NewFloatEncoder returns a new FloatEncoder.
func NewFloatEncoder() *FloatEncoder {
	buf := make([]float64, 0, tsdb.DefaultMaxPointsPerBlock)
	s := FloatEncoder{
		buf,
	}

	return &s
}

// Reset sets the encoder back to its initial state.
func (s *FloatEncoder) Reset() {
	s.buf = s.buf[0:0]
}

// Bytes returns a copy of the underlying byte buffer used in the encoder.
func (s *FloatEncoder) Bytes() ([]byte, error) {
	var res []byte
	if len(s.buf) <= 20 {
		res = make([]byte, len(s.buf)*8+1)
		res[0] = floatCompressedNone
		C.memcpy(
			unsafe.Pointer(&res[1]),
			unsafe.Pointer(&s.buf[0]),
			C.size_t(len(s.buf)*8))
	} else {
		// fmt.Printf("Calling SZ_compress %v %v\n", &s.buf[0], len(s.buf))
		msg := C.compress(unsafe.Pointer(&s.buf[0]), C.size_t(len(s.buf)), C.double(error_bound))
		defer C.free(unsafe.Pointer(msg))

		// fmt.Printf("Float encoder out: %v (%v)\n", out, outSize);
		res = make([]byte, int(msg.size)+3)
		res[0] = floatCompressedSZ
		res[1] = byte(len(s.buf) & 0xff)
		res[2] = byte(len(s.buf) >> 8)
		C.memcpy(
			unsafe.Pointer(&res[3]),
			unsafe.Pointer(&msg.data[0]),
			C.size_t(msg.size))
	}
	return res, nil
}

// Flush indicates there are no more values to encode.
func (s *FloatEncoder) Flush() {}

// Write encodes v to the underlying buffer.
func (s *FloatEncoder) Write(v float64) {
	s.buf = append(s.buf, v)
}

// FloatDecoder decodes a byte slice into multiple float64 values.
type FloatDecoder struct {
	buf []float64
	cur uint
	err error
}

// SetBytes initializes the decoder with b. Must call before calling Next().
func (it *FloatDecoder) SetBytes(b []byte) error {
	it.cur = 0
	if b[0] == floatCompressedNone {
		length := (len(b) - 1) / 8
		if cap(it.buf) < length {
			it.buf = make([]float64, (len(b)-1)/8)
		} else {
			it.buf = it.buf[:length]
		}
		C.memcpy(
			unsafe.Pointer(&it.buf[0]),
			unsafe.Pointer(&b[1]),
			C.size_t(len(b)-1))
	} else if b[0] == floatCompressedSZ {
		blen := uint(b[2])
		blen = (blen << 8) + uint(b[1])
		if cap(it.buf) < int(blen) {
			it.buf = make([]float64, blen)
		} else {
			it.buf = it.buf[:blen]
		}
		msg := C.decompress(
			(unsafe.Pointer(&b[3])),
			C.size_t(len(b)-3),
			C.size_t(blen))
		defer C.free(unsafe.Pointer(msg))
		C.memcpy(unsafe.Pointer(&it.buf[0]),
			unsafe.Pointer(&msg.data[0]),
			C.size_t(blen<<3))
	} else {
		it.err = errors.New("unknown compression type")
		return it.err
	}
	return nil
}

// Next returns true if there are remaining values to read.
func (it *FloatDecoder) Next() bool {
	return it.cur < uint(len(it.buf))
}

// Values returns the current float64 value.
func (it *FloatDecoder) Values() float64 {
	it.cur += 1
	return it.buf[it.cur-1]
}

// Error returns the current decoding error.
func (it *FloatDecoder) Error() error {
	return it.err
}
