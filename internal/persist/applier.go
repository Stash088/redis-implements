package persist

import (
	"fmt"
	"io"

	"github.com/amir/redis-implements/internal/replay"
	"github.com/amir/redis-implements/internal/resp"
	"github.com/amir/redis-implements/internal/store"
)

// LoadAOF replays a RESP command log into the given stores. A truncated
// trailing command is tolerated; other read errors abort the load.
func LoadAOF(r io.Reader, stores []*store.Store) error {
	rd := resp.NewReader(r)
	ap := replay.NewApplier(stores, RestoreValue)
	for {
		v, err := rd.ReadValue()
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil // clean end or truncated tail
			}
			return fmt.Errorf("persist: aof read: %w", err)
		}
		if v.Kind != resp.KindArray || v.IsNull {
			return fmt.Errorf("persist: aof contains a non-command frame")
		}
		args := make([][]byte, 0, len(v.Array))
		for i := range v.Array {
			el := v.Array[i]
			if el.Kind != resp.KindBulkString || el.IsNull {
				return fmt.Errorf("persist: aof command arguments must be bulk strings")
			}
			args = append(args, el.Str)
		}
		if err := ap.Apply(args); err != nil {
			return err
		}
	}
}
