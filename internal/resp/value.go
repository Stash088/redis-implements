package resp

// Kind describes the RESP type of a value.
type Kind byte

const (
	KindSimpleString Kind = '+'
	KindError        Kind = '-'
	KindInteger      Kind = ':'
	KindBulkString   Kind = '$'
	KindArray        Kind = '*'
)

// Value is a decoded RESP message.
//
// Depending on Kind:
//   - KindSimpleString, KindError: Str holds the payload.
//   - KindInteger: Int holds the value.
//   - KindBulkString: Str holds the payload; IsNull marks $-1.
//   - KindArray: Array holds the elements; IsNull marks *-1.
type Value struct {
	Kind   Kind
	Str    []byte
	Int    int64
	Array  []Value
	IsNull bool
}

// NewSimpleString returns a simple string value.
func NewSimpleString(s string) Value {
	return Value{Kind: KindSimpleString, Str: []byte(s)}
}

// NewError returns an error value. msg must not include the leading "-".
func NewError(msg string) Value {
	return Value{Kind: KindError, Str: []byte(msg)}
}

// NewInteger returns an integer value.
func NewInteger(n int64) Value {
	return Value{Kind: KindInteger, Int: n}
}

// NewBulkString returns a bulk string value. A nil payload produces a null bulk string.
func NewBulkString(b []byte) Value {
	if b == nil {
		return Value{Kind: KindBulkString, IsNull: true}
	}
	return Value{Kind: KindBulkString, Str: b}
}

// NewArray returns an array value. A nil elements slice produces a null array.
func NewArray(elems []Value) Value {
	if elems == nil {
		return Value{Kind: KindArray, IsNull: true}
	}
	return Value{Kind: KindArray, Array: elems}
}

// IsNil reports whether the value is a RESP null (null bulk string or null array).
func (v Value) IsNil() bool {
	return v.IsNull
}
