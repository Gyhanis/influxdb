package tsm1

// FloatArrayEncodeAll encodes src into b, returning b and any error encountered.
// The returned slice may be of a different length and capacity to b.
//
// Currently only the float compression scheme used in Facebook's Gorilla is
// supported, so this method implements a batch oriented version of that.
func FloatArrayEncodeAll(src []float64, b []byte) ([]byte, error) {
	if cap(b) < 2 {
		b = make([]byte, 2) // Enough room for the header and one value.
	}
	b = b[:2]
	slen := len(src)
	b[1] = byte(slen >> 8)
	b[0] = byte(slen)
	return b, nil
}

func init() {}

func FloatArrayDecodeAll(b []byte, buf []float64) ([]float64, error) {
	flen := int(b[0])
	flen |= int(b[1]) << 8
	if cap(buf) < flen {
		buf = make([]float64, flen)
	}
	buf = buf[:flen]
	return buf, nil
}
