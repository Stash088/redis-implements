package resp

import "strconv"

// EncodeCommand serializes a command as a RESP array of bulk strings.
func EncodeCommand(args [][]byte) []byte {
	return AppendCommand(make([]byte, 0, 64), args)
}

// AppendCommand serializes a command as a RESP array of bulk strings and
// appends it to dst. The buffer is reused, so appending a command does not
// allocate.
func AppendCommand(dst []byte, args [][]byte) []byte {
	dst = append(dst, '*')
	dst = strconv.AppendInt(dst, int64(len(args)), 10)
	dst = append(dst, '\r', '\n')
	for _, a := range args {
		dst = append(dst, '$')
		dst = strconv.AppendInt(dst, int64(len(a)), 10)
		dst = append(dst, '\r', '\n')
		dst = append(dst, a...)
		dst = append(dst, '\r', '\n')
	}
	return dst
}
