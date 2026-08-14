package persist

import "fmt"

// lzfDecompress implements the LZF decompression algorithm used by Redis for
// compressed strings in RDB files (liblzf). It writes exactly len(out) bytes
// into out and returns an error if the input stream overruns out.
func lzfDecompress(in, out []byte) error {
	ip := 0
	op := 0
	for ip < len(in) {
		ctrl := int(in[ip])
		ip++
		if ctrl < 32 {
			// literal run of ctrl+1 bytes
			n := ctrl + 1
			if ip+n > len(in) || op+n > len(out) {
				return fmt.Errorf("persist: lzf literal overrun")
			}
			copy(out[op:], in[ip:ip+n])
			ip += n
			op += n
			continue
		}
		// back-reference run
		length := ctrl >> 5
		if length == 7 {
			if ip >= len(in) {
				return fmt.Errorf("persist: lzf truncated back-reference length")
			}
			length += int(in[ip])
			ip++
		}
		if ip >= len(in) {
			return fmt.Errorf("persist: lzf truncated back-reference offset")
		}
		ref := op - ((ctrl & 0x1F) << 8) - int(in[ip]) - 1
		ip++
		if ref < 0 {
			return fmt.Errorf("persist: lzf invalid back-reference")
		}
		n := length + 2
		if op+n > len(out) {
			return fmt.Errorf("persist: lzf output overrun")
		}
		for i := 0; i < n; i++ {
			out[op+i] = out[ref+i]
		}
		op += n
	}
	if op != len(out) {
		return fmt.Errorf("persist: lzf decompressed size mismatch (%d != %d)", op, len(out))
	}
	return nil
}
