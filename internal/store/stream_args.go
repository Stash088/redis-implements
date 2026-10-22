package store

import (
	"strings"
	"time"
)

// StreamGroupCommand is a parsed XGROUP command.
type StreamGroupCommand struct {
	// Sub is the subcommand: CREATE, DESTROY, CREATECONSUMER, DELCONSUMER or
	// SETID.
	Sub string
	// Key is the key of the stream.
	Key []byte
	// Group is the name of the consumer group.
	Group string
	// Consumer is the name of the consumer (CREATECONSUMER, DELCONSUMER).
	Consumer string
	// ID is the identifier of the last entry delivered to the group.
	ID StreamID
	// HasID tells whether the identifier was given.
	HasID bool
	// IDIsDollar tells whether the identifier was the "$" form, which stands
	// for the last entry of the stream.
	IDIsDollar bool
	// EntriesRead is the value of the ENTRIESREAD option.
	EntriesRead uint64
	// SetEntriesRead tells whether the ENTRIESREAD option was given.
	SetEntriesRead bool
	// MkStream asks to create the stream when it is missing.
	MkStream bool
}

// ParseStreamGroupArgs parses the arguments of an XGROUP command, without the
// name of the command.
func ParseStreamGroupArgs(args [][]byte) (StreamGroupCommand, error) {
	var cmd StreamGroupCommand
	if len(args) < 1 {
		return cmd, ErrStreamSyntax
	}
	cmd.Sub = strings.ToUpper(string(args[0]))
	switch cmd.Sub {
	case "CREATE":
		if len(args) < 4 {
			return cmd, ErrStreamSyntax
		}
		cmd.Key = args[1]
		cmd.Group = string(args[2])
		for i := 4; i < len(args); {
			switch strings.ToUpper(string(args[i])) {
			case "MKSTREAM":
				cmd.MkStream = true
				i++
			case "ENTRIESREAD":
				if i+1 >= len(args) {
					return cmd, ErrStreamSyntax
				}
				n, ok := parseStreamUint(string(args[i+1]))
				if !ok {
					return cmd, ErrStreamNotInteger
				}
				cmd.EntriesRead = n
				cmd.SetEntriesRead = true
				i += 2
			default:
				return cmd, ErrStreamSyntax
			}
		}
		id, err := parseStreamGroupID(string(args[3]))
		if err != nil {
			return cmd, err
		}
		cmd.ID = id
		cmd.HasID = true
		cmd.IDIsDollar = string(args[3]) == "$"
	case "SETID":
		if len(args) < 4 {
			return cmd, ErrStreamSyntax
		}
		cmd.Key = args[1]
		cmd.Group = string(args[2])
		for i := 4; i < len(args); {
			if !strings.EqualFold(string(args[i]), "ENTRIESREAD") || i+1 >= len(args) {
				return cmd, ErrStreamSyntax
			}
			n, ok := parseStreamUint(string(args[i+1]))
			if !ok {
				return cmd, ErrStreamNotInteger
			}
			cmd.EntriesRead = n
			cmd.SetEntriesRead = true
			i += 2
		}
		id, err := parseStreamGroupID(string(args[3]))
		if err != nil {
			return cmd, err
		}
		cmd.ID = id
		cmd.HasID = true
		cmd.IDIsDollar = string(args[3]) == "$"
	case "DESTROY":
		if len(args) != 3 {
			return cmd, ErrStreamSyntax
		}
		cmd.Key = args[1]
		cmd.Group = string(args[2])
	case "CREATECONSUMER":
		if len(args) != 4 {
			return cmd, ErrStreamSyntax
		}
		cmd.Key = args[1]
		cmd.Group = string(args[2])
		cmd.Consumer = string(args[3])
	case "DELCONSUMER":
		if len(args) != 4 {
			return cmd, ErrStreamSyntax
		}
		cmd.Key = args[1]
		cmd.Group = string(args[2])
		cmd.Consumer = string(args[3])
	default:
		return cmd, ErrStreamSyntax
	}
	return cmd, nil
}

// parseStreamGroupID parses the identifier of a consumer group.
func parseStreamGroupID(arg string) (StreamID, error) {
	if arg == "$" {
		return StreamID{}, nil
	}
	id, ok := ParseStreamIDStrict(arg, 0, nil)
	if !ok {
		return StreamID{}, ErrInvalidStreamID
	}
	return id, nil
}

// StreamClaimCommand is a parsed XCLAIM command.
type StreamClaimCommand struct {
	// Key is the key of the stream.
	Key []byte
	// Group is the name of the consumer group.
	Group string
	// Consumer is the name of the consumer that gets the entries.
	Consumer string
	// MinIdle is the minimum idle time of the claimed entries.
	MinIdle time.Duration
	// IDs holds the identifiers of the claimed entries.
	IDs []StreamID
	// Options holds the options of the command.
	Options StreamClaimOptions
}

// ParseStreamClaimArgs parses the arguments of an XCLAIM command, without the
// name of the command.
func ParseStreamClaimArgs(args [][]byte) (StreamClaimCommand, error) {
	var cmd StreamClaimCommand
	if len(args) < 5 {
		return cmd, ErrStreamSyntax
	}
	cmd.Key = args[0]
	cmd.Group = string(args[1])
	cmd.Consumer = string(args[2])
	ms, ok := parseStreamUint(string(args[3]))
	if !ok {
		return cmd, ErrStreamNotInteger
	}
	cmd.MinIdle = time.Duration(ms) * time.Millisecond

	// The identifiers come first, the options follow them.
	i := 4
	for ; i < len(args); i++ {
		id, ok := ParseStreamIDStrict(string(args[i]), 0, nil)
		if !ok {
			break
		}
		cmd.IDs = append(cmd.IDs, id)
	}
	if len(cmd.IDs) == 0 {
		return cmd, ErrInvalidStreamID
	}
	for ; i < len(args); i++ {
		opt := strings.ToUpper(string(args[i]))
		more := len(args) - i - 1
		switch opt {
		case "FORCE":
			cmd.Options.Force = true
		case "JUSTID":
			cmd.Options.JustID = true
		case "IDLE":
			if more < 1 {
				return cmd, ErrStreamSyntax
			}
			n, ok := parseStreamUint(string(args[i+1]))
			if !ok {
				return cmd, ErrStreamNotInteger
			}
			cmd.Options.Idle = time.Duration(n) * time.Millisecond
			cmd.Options.HasIdle = true
			i++
		case "TIME":
			if more < 1 {
				return cmd, ErrStreamSyntax
			}
			n, ok := parseStreamUint(string(args[i+1]))
			if !ok {
				return cmd, ErrStreamNotInteger
			}
			cmd.Options.Time = time.UnixMilli(int64(n))
			cmd.Options.HasTime = true
			i++
		case "RETRYCOUNT":
			if more < 1 {
				return cmd, ErrStreamSyntax
			}
			n, ok := parseStreamUint(string(args[i+1]))
			if !ok {
				return cmd, ErrStreamNotInteger
			}
			cmd.Options.RetryCount = int64(n)
			cmd.Options.HasRetryCount = true
			i++
		case "LASTID":
			if more < 1 {
				return cmd, ErrStreamSyntax
			}
			id, ok := ParseStreamIDStrict(string(args[i+1]), 0, nil)
			if !ok {
				return cmd, ErrInvalidStreamID
			}
			cmd.Options.LastID = &id
			i++
		default:
			return cmd, ErrStreamSyntax
		}
	}
	return cmd, nil
}
