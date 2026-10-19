package resp

// Kind describes the RESP type of a value.
type Kind byte

const (
	KindSimpleString Kind = '+'
	KindError        Kind = '-'
	KindInteger      Kind = ':'
	KindBulkString   Kind = '$'
	KindArray        Kind = '*'

	// RESP3 types.
	KindMap       Kind = '%'
	KindSet       Kind = '~'
	KindDouble    Kind = ','
	KindBigNumber Kind = '('
	KindVerbatim  Kind = '='
	KindPush      Kind = '>'
)

// Value is a decoded RESP message.
//
// Depending on Kind:
//   - KindSimpleString, KindError: Str holds the payload.
//   - KindInteger: Int holds the value.
//   - KindBulkString: Str holds the payload; IsNull marks $-1.
//   - KindArray, KindPush: Array holds the elements; IsNull marks *-1.
//   - KindMap: Array holds the elements as alternating key/value pairs.
//   - KindSet: Array holds the elements.
//   - KindDouble: Float holds the value.
//   - KindBigNumber: Str holds the decimal representation.
//   - KindVerbatim: Str holds the payload including its "txt:" prefix.
type Value struct {
	Kind   Kind
	Str    []byte
	Int    int64
	Float  float64
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

// NewMap returns a map value. The elements are alternating keys and values.
func NewMap(elems []Value) Value {
	return Value{Kind: KindMap, Array: elems}
}

// NewSet returns a set value.
func NewSet(elems []Value) Value {
	return Value{Kind: KindSet, Array: elems}
}

// NewDouble returns a double value.
func NewDouble(f float64) Value {
	return Value{Kind: KindDouble, Float: f}
}

// NewBigNumber returns a big number value. s must hold the decimal digits.
func NewBigNumber(s string) Value {
	return Value{Kind: KindBigNumber, Str: []byte(s)}
}

// NewVerbatim returns a verbatim string value. The payload must include the
// format prefix ("txt:", "mkd:" or "json:"), as Redis does.
func NewVerbatim(s string) Value {
	return Value{Kind: KindVerbatim, Str: []byte(s)}
}

// NewPush returns a push value (RESP3 out-of-band message).
func NewPush(elems []Value) Value {
	return Value{Kind: KindPush, Array: elems}
}

// IsNil reports whether the value is a RESP null (null bulk string or null array).
func (v Value) IsNil() bool {
	return v.IsNull
}
