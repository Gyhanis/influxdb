package tsm1

/*
This code is originally from: https://github.com/dgryski/go-tsz and has been modified to remove
the timestamp compression functionality.

It implements the float compression as presented in: http://www.vldb.org/pvldb/vol8/p1816-teller.pdf.
This implementation uses a sentinel value of NaN which means that float64 NaN cannot be stored using
this version.
*/

// Note: an uncompressed format is not yet implemented.
// floatCompressedGorilla is a compressed format using the gorilla paper encoding
const floatCompressedGorilla = 1

// uvnan is the constant returned from math.NaN().
const uvnan = 0x7FF8000000000001

// FloatEncoder encodes multiple float64s into a byte slice.
type FloatEncoder struct {
	err   error
	count int
}

// NewFloatEncoder returns a new FloatEncoder.
func NewFloatEncoder() *FloatEncoder {
	s := FloatEncoder{}
	return &s
}

// Reset sets the encoder back to its initial state.
func (s *FloatEncoder) Reset() {
	s.count = 0
}

// Bytes returns a copy of the underlying byte buffer used in the encoder.
func (s *FloatEncoder) Bytes() ([]byte, error) {
	b := make([]byte, 2)
	b[1] = byte(s.count >> 8)
	b[0] = byte(s.count)
	return b, nil
}

// Flush indicates there are no more values to encode.
func (s *FloatEncoder) Flush() {}

// Write encodes v to the underlying buffer.
func (s *FloatEncoder) Write(v float64) {
	s.count += 1
}

// FloatDecoder decodes a byte slice into multiple float64 values.
type FloatDecoder struct {
	count int
	cur   int

	err error
}

// SetBytes initializes the decoder with b. Must call before calling Next().
func (it *FloatDecoder) SetBytes(b []byte) error {
	it.count = int(b[0])
	it.count |= int(b[1]) << 8
	it.cur = 0
	it.err = nil
	return nil
}

// Next returns true if there are remaining values to read.
func (it *FloatDecoder) Next() bool {
	return it.cur < it.count
}

// Values returns the current float64 value.
func (it *FloatDecoder) Values() float64 {
	return 0
}

// Error returns the current decoding error.
func (it *FloatDecoder) Error() error {
	return it.err
}
